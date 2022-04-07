// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package interpreter

import (
	"fmt"
	"strings"

	"github.com/google/cel-go/common/containers"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/interpreter/functions"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// interpretablePlanner creates an Interpretable evaluation plan from a proto Expr value.
type interpretablePlanner interface {
	// Plan generates an Interpretable value (or error) from the input proto Expr.
	Plan(expr *exprpb.Expr) (Interpretable, error)
}

// newPlanner creates an interpretablePlanner which references a Dispatcher, TypeProvider,
// TypeAdapter, Container, and CheckedExpr value. These pieces of data are used to resolve
// functions, types, and namespaced identifiers at plan time rather than at runtime since
// it only needs to be done once and may be semi-expensive to compute.
func newPlanner(disp Dispatcher,
	provider ref.TypeProvider,
	adapter ref.TypeAdapter,
	attrFactory AttributeFactory,
	cont *containers.Container,
	checked *exprpb.CheckedExpr,
	decorators ...InterpretableDecorator) interpretablePlanner {
	return &planner{
		disp:        disp,
		provider:    provider,
		adapter:     adapter,
		attrFactory: attrFactory,
		container:   cont,
		refMap:      checked.GetReferenceMap(),
		typeMap:     checked.GetTypeMap(),
		decorators:  decorators,
	}
}

// newUncheckedPlanner creates an interpretablePlanner which references a Dispatcher, TypeProvider,
// TypeAdapter, and Container to resolve functions and types at plan time. Namespaces present in
// Select expressions are resolved lazily at evaluation time.
func newUncheckedPlanner(disp Dispatcher,
	provider ref.TypeProvider,
	adapter ref.TypeAdapter,
	attrFactory AttributeFactory,
	cont *containers.Container,
	decorators ...InterpretableDecorator) interpretablePlanner {
	return &planner{
		disp:        disp,
		provider:    provider,
		adapter:     adapter,
		attrFactory: attrFactory,
		container:   cont,
		refMap:      make(map[int64]*exprpb.Reference),
		typeMap:     make(map[int64]*exprpb.Type),
		decorators:  decorators,
	}
}

// planner is an implementatio of the interpretablePlanner interface.
type planner struct {
	disp        Dispatcher
	provider    ref.TypeProvider
	adapter     ref.TypeAdapter
	attrFactory AttributeFactory
	container   *containers.Container
	refMap      map[int64]*exprpb.Reference
	typeMap     map[int64]*exprpb.Type
	decorators  []InterpretableDecorator
}

// Plan implements the interpretablePlanner interface. This implementation of the Plan method also
// applies decorators to each Interpretable generated as part of the overall plan. Decorators are
// useful for layering functionality into the evaluation that is not natively understood by CEL,
// such as state-tracking, expression re-write, and possibly efficient thread-safe memoization of
// repeated expressions.
func (p *planner) Plan(expr *exprpb.Expr) (Interpretable, error) {
	switch expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		return p.decorate(p.planCall(expr))
	case *exprpb.Expr_IdentExpr:
		return p.decorate(p.planIdent(expr))
	case *exprpb.Expr_SelectExpr:
		return p.decorate(p.planSelect(expr))
	case *exprpb.Expr_ListExpr:
		return p.decorate(p.planCreateList(expr))
	case *exprpb.Expr_StructExpr:
		return p.decorate(p.planCreateStruct(expr))
	case *exprpb.Expr_ComprehensionExpr:
		return p.decorate(p.planComprehension(expr))
	case *exprpb.Expr_ConstExpr:
		return p.decorate(p.planConst(expr))
	}
	return nil, fmt.Errorf("unsupported expr: %v", expr)
}

// decorate applies the InterpretableDecorator functions to the given Interpretable.
// Both the Interpretable and error generated by a Plan step are accepted as arguments
// for convenience.
func (p *planner) decorate(i Interpretable, err error) (Interpretable, error) {
	if err != nil {
		return nil, err
	}
	for _, dec := range p.decorators {
		i, err = dec(i)
		if err != nil {
			return nil, err
		}
	}
	return i, nil
}

// planIdent creates an Interpretable that resolves an identifier from an Activation.
func (p *planner) planIdent(expr *exprpb.Expr) (Interpretable, error) {
	// Establish whether the identifier is in the reference map.
	if identRef, found := p.refMap[expr.GetId()]; found {
		return p.planCheckedIdent(expr.GetId(), identRef)
	}
	// Create the possible attribute list for the unresolved reference.
	ident := expr.GetIdentExpr()
	return &evalAttr{
		adapter: p.adapter,
		attr:    p.attrFactory.MaybeAttribute(expr.GetId(), ident.Name),
	}, nil
}

func (p *planner) planCheckedIdent(id int64, identRef *exprpb.Reference) (Interpretable, error) {
	// Plan a constant reference if this is the case for this simple identifier.
	if identRef.Value != nil {
		return p.Plan(&exprpb.Expr{Id: id,
			ExprKind: &exprpb.Expr_ConstExpr{
				ConstExpr: identRef.Value,
			}})
	}

	// Check to see whether the type map indicates this is a type name. All types should be
	// registered with the provider.
	cType := p.typeMap[id]
	if cType.GetType() != nil {
		cVal, found := p.provider.FindIdent(identRef.Name)
		if !found {
			return nil, fmt.Errorf("reference to undefined type: %s", identRef.Name)
		}
		return NewConstValue(id, cVal), nil
	}

	// Otherwise, return the attribute for the resolved identifier name.
	return &evalAttr{
		adapter: p.adapter,
		attr:    p.attrFactory.AbsoluteAttribute(id, identRef.Name),
	}, nil
}

// planSelect creates an Interpretable with either:
//  a) selects a field from a map or proto.
//  b) creates a field presence test for a select within a has() macro.
//  c) resolves the select expression to a namespaced identifier.
func (p *planner) planSelect(expr *exprpb.Expr) (Interpretable, error) {
	// If the Select id appears in the reference map from the CheckedExpr proto then it is either
	// a namespaced identifier or enum value.
	if identRef, found := p.refMap[expr.GetId()]; found {
		return p.planCheckedIdent(expr.GetId(), identRef)
	}

	sel := expr.GetSelectExpr()
	// Plan the operand evaluation.
	op, err := p.Plan(sel.GetOperand())
	if err != nil {
		return nil, err
	}

	// Determine the field type if this is a proto message type.
	var fieldType *ref.FieldType
	opType := p.typeMap[sel.GetOperand().GetId()]
	if opType.GetMessageType() != "" {
		ft, found := p.provider.FindFieldType(opType.GetMessageType(), sel.Field)
		if found && ft.IsSet != nil && ft.GetFrom != nil {
			fieldType = ft
		}
	}

	// If the Select was marked TestOnly, this is a presence test.
	//
	// Note: presence tests are defined for structured (e.g. proto) and dynamic values (map, json)
	// as follows:
	//  - True if the object field has a non-default value, e.g. obj.str != ""
	//  - True if the dynamic value has the field defined, e.g. key in map
	//
	// However, presence tests are not defined for qualified identifier names with primitive types.
	// If a string named 'a.b.c' is declared in the environment and referenced within `has(a.b.c)`,
	// it is not clear whether has should error or follow the convention defined for structured
	// values.
	if sel.TestOnly {
		// Return the test only eval expression.
		return &evalTestOnly{
			id:        expr.Id,
			field:     types.String(sel.Field),
			fieldType: fieldType,
			op:        op,
		}, nil
	}
	// Build a qualifier.
	qual, err := p.attrFactory.NewQualifier(
		opType, expr.Id, sel.Field)
	if err != nil {
		return nil, err
	}
	// Lastly, create a field selection Interpretable.
	attr, isAttr := op.(InterpretableAttribute)
	if isAttr {
		_, err = attr.AddQualifier(qual)
		return attr, err
	}

	relAttr, err := p.relativeAttr(op.ID(), op)
	if err != nil {
		return nil, err
	}
	_, err = relAttr.AddQualifier(qual)
	if err != nil {
		return nil, err
	}
	return relAttr, nil
}

// planCall creates a callable Interpretable while specializing for common functions and invocation
// patterns. Specifically, conditional operators &&, ||, ?:, and (in)equality functions result in
// optimized Interpretable values.
func (p *planner) planCall(expr *exprpb.Expr) (Interpretable, error) {
	call := expr.GetCallExpr()
	target, fnName, oName := p.resolveFunction(expr)
	argCount := len(call.GetArgs())
	var offset int
	if target != nil {
		argCount++
		offset++
	}

	args := make([]Interpretable, argCount)
	if target != nil {
		arg, err := p.Plan(target)
		if err != nil {
			return nil, err
		}
		args[0] = arg
	}
	for i, argExpr := range call.GetArgs() {
		arg, err := p.Plan(argExpr)
		if err != nil {
			return nil, err
		}
		args[i+offset] = arg
	}

	// Generate specialized Interpretable operators by function name if possible.
	switch fnName {
	case operators.LogicalAnd:
		return p.planCallLogicalAnd(expr, args)
	case operators.LogicalOr:
		return p.planCallLogicalOr(expr, args)
	case operators.Conditional:
		return p.planCallConditional(expr, args)
	case operators.Equals:
		return p.planCallEqual(expr, args)
	case operators.NotEquals:
		return p.planCallNotEqual(expr, args)
	case operators.Index:
		return p.planCallIndex(expr, args)
	}

	// Otherwise, generate Interpretable calls specialized by argument count.
	// Try to find the specific function by overload id.
	var fnDef *functions.Overload
	if oName != "" {
		fnDef, _ = p.disp.FindOverload(oName)
	}
	// If the overload id couldn't resolve the function, try the simple function name.
	if fnDef == nil {
		fnDef, _ = p.disp.FindOverload(fnName)
	}
	switch argCount {
	case 0:
		return p.planCallZero(expr, fnName, oName, fnDef)
	case 1:
		return p.planCallUnary(expr, fnName, oName, fnDef, args)
	case 2:
		return p.planCallBinary(expr, fnName, oName, fnDef, args)
	default:
		return p.planCallVarArgs(expr, fnName, oName, fnDef, args)
	}
}

// planCallZero generates a zero-arity callable Interpretable.
func (p *planner) planCallZero(expr *exprpb.Expr,
	function string,
	overload string,
	impl *functions.Overload) (Interpretable, error) {
	if impl == nil || impl.Function == nil {
		return nil, fmt.Errorf("no such overload: %s()", function)
	}
	return &evalZeroArity{
		id:       expr.Id,
		function: function,
		overload: overload,
		impl:     impl.Function,
	}, nil
}

// planCallUnary generates a unary callable Interpretable.
func (p *planner) planCallUnary(expr *exprpb.Expr,
	function string,
	overload string,
	impl *functions.Overload,
	args []Interpretable) (Interpretable, error) {
	var fn functions.UnaryOp
	var trait int
	if impl != nil {
		if impl.Unary == nil {
			return nil, fmt.Errorf("no such overload: %s(arg)", function)
		}
		fn = impl.Unary
		trait = impl.OperandTrait
	}
	return &evalUnary{
		id:       expr.Id,
		function: function,
		overload: overload,
		arg:      args[0],
		trait:    trait,
		impl:     fn,
	}, nil
}

// planCallBinary generates a binary callable Interpretable.
func (p *planner) planCallBinary(expr *exprpb.Expr,
	function string,
	overload string,
	impl *functions.Overload,
	args []Interpretable) (Interpretable, error) {
	var fn functions.BinaryOp
	var trait int
	if impl != nil {
		if impl.Binary == nil {
			return nil, fmt.Errorf("no such overload: %s(lhs, rhs)", function)
		}
		fn = impl.Binary
		trait = impl.OperandTrait
	}
	return &evalBinary{
		id:       expr.Id,
		function: function,
		overload: overload,
		lhs:      args[0],
		rhs:      args[1],
		trait:    trait,
		impl:     fn,
	}, nil
}

// planCallVarArgs generates a variable argument callable Interpretable.
func (p *planner) planCallVarArgs(expr *exprpb.Expr,
	function string,
	overload string,
	impl *functions.Overload,
	args []Interpretable) (Interpretable, error) {
	var fn functions.FunctionOp
	var trait int
	if impl != nil {
		if impl.Function == nil {
			return nil, fmt.Errorf("no such overload: %s(...)", function)
		}
		fn = impl.Function
		trait = impl.OperandTrait
	}
	return &evalVarArgs{
		id:       expr.Id,
		function: function,
		overload: overload,
		args:     args,
		trait:    trait,
		impl:     fn,
	}, nil
}

// planCallEqual generates an equals (==) Interpretable.
func (p *planner) planCallEqual(expr *exprpb.Expr,
	args []Interpretable) (Interpretable, error) {
	return &evalEq{
		id:  expr.Id,
		lhs: args[0],
		rhs: args[1],
	}, nil
}

// planCallNotEqual generates a not equals (!=) Interpretable.
func (p *planner) planCallNotEqual(expr *exprpb.Expr,
	args []Interpretable) (Interpretable, error) {
	return &evalNe{
		id:  expr.Id,
		lhs: args[0],
		rhs: args[1],
	}, nil
}

// planCallLogicalAnd generates a logical and (&&) Interpretable.
func (p *planner) planCallLogicalAnd(expr *exprpb.Expr,
	args []Interpretable) (Interpretable, error) {
	return &evalAnd{
		id:  expr.Id,
		lhs: args[0],
		rhs: args[1],
	}, nil
}

// planCallLogicalOr generates a logical or (||) Interpretable.
func (p *planner) planCallLogicalOr(expr *exprpb.Expr,
	args []Interpretable) (Interpretable, error) {
	return &evalOr{
		id:  expr.Id,
		lhs: args[0],
		rhs: args[1],
	}, nil
}

// planCallConditional generates a conditional / ternary (c ? t : f) Interpretable.
func (p *planner) planCallConditional(expr *exprpb.Expr,
	args []Interpretable) (Interpretable, error) {
	cond := args[0]

	t := args[1]
	var tAttr Attribute
	truthyAttr, isTruthyAttr := t.(InterpretableAttribute)
	if isTruthyAttr {
		tAttr = truthyAttr.Attr()
	} else {
		tAttr = p.attrFactory.RelativeAttribute(t.ID(), t)
	}

	f := args[2]
	var fAttr Attribute
	falsyAttr, isFalsyAttr := f.(InterpretableAttribute)
	if isFalsyAttr {
		fAttr = falsyAttr.Attr()
	} else {
		fAttr = p.attrFactory.RelativeAttribute(f.ID(), f)
	}

	return &evalAttr{
		adapter: p.adapter,
		attr:    p.attrFactory.ConditionalAttribute(expr.Id, cond, tAttr, fAttr),
	}, nil
}

// planCallIndex either extends an attribute with the argument to the index operation, or creates
// a relative attribute based on the return of a function call or operation.
func (p *planner) planCallIndex(expr *exprpb.Expr,
	args []Interpretable) (Interpretable, error) {
	op := args[0]
	ind := args[1]
	opAttr, err := p.relativeAttr(op.ID(), op)
	if err != nil {
		return nil, err
	}
	opType := p.typeMap[expr.GetCallExpr().GetTarget().GetId()]
	indConst, isIndConst := ind.(InterpretableConst)
	if isIndConst {
		qual, err := p.attrFactory.NewQualifier(
			opType, expr.GetId(), indConst.Value())
		if err != nil {
			return nil, err
		}
		_, err = opAttr.AddQualifier(qual)
		return opAttr, err
	}
	indAttr, isIndAttr := ind.(InterpretableAttribute)
	if isIndAttr {
		qual, err := p.attrFactory.NewQualifier(
			opType, expr.GetId(), indAttr)
		if err != nil {
			return nil, err
		}
		_, err = opAttr.AddQualifier(qual)
		return opAttr, err
	}
	indQual, err := p.relativeAttr(expr.GetId(), ind)
	if err != nil {
		return nil, err
	}
	_, err = opAttr.AddQualifier(indQual)
	return opAttr, err
}

// planCreateList generates a list construction Interpretable.
func (p *planner) planCreateList(expr *exprpb.Expr) (Interpretable, error) {
	list := expr.GetListExpr()
	elems := make([]Interpretable, len(list.GetElements()))
	for i, elem := range list.GetElements() {
		elemVal, err := p.Plan(elem)
		if err != nil {
			return nil, err
		}
		elems[i] = elemVal
	}
	return &evalList{
		id:      expr.Id,
		elems:   elems,
		adapter: p.adapter,
	}, nil
}

// planCreateStruct generates a map or object construction Interpretable.
func (p *planner) planCreateStruct(expr *exprpb.Expr) (Interpretable, error) {
	str := expr.GetStructExpr()
	if len(str.MessageName) != 0 {
		return p.planCreateObj(expr)
	}
	entries := str.GetEntries()
	keys := make([]Interpretable, len(entries))
	vals := make([]Interpretable, len(entries))
	for i, entry := range entries {
		keyVal, err := p.Plan(entry.GetMapKey())
		if err != nil {
			return nil, err
		}
		keys[i] = keyVal

		valVal, err := p.Plan(entry.GetValue())
		if err != nil {
			return nil, err
		}
		vals[i] = valVal
	}
	return &evalMap{
		id:      expr.Id,
		keys:    keys,
		vals:    vals,
		adapter: p.adapter,
	}, nil
}

// planCreateObj generates an object construction Interpretable.
func (p *planner) planCreateObj(expr *exprpb.Expr) (Interpretable, error) {
	obj := expr.GetStructExpr()
	typeName, defined := p.resolveTypeName(obj.MessageName)
	if !defined {
		return nil, fmt.Errorf("unknown type: %s", typeName)
	}
	entries := obj.GetEntries()
	fields := make([]string, len(entries))
	vals := make([]Interpretable, len(entries))
	for i, entry := range entries {
		fields[i] = entry.GetFieldKey()
		val, err := p.Plan(entry.GetValue())
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return &evalObj{
		id:       expr.Id,
		typeName: typeName,
		fields:   fields,
		vals:     vals,
		provider: p.provider,
	}, nil
}

// planComprehension generates an Interpretable fold operation.
func (p *planner) planComprehension(expr *exprpb.Expr) (Interpretable, error) {
	fold := expr.GetComprehensionExpr()
	accu, err := p.Plan(fold.GetAccuInit())
	if err != nil {
		return nil, err
	}
	iterRange, err := p.Plan(fold.GetIterRange())
	if err != nil {
		return nil, err
	}
	cond, err := p.Plan(fold.GetLoopCondition())
	if err != nil {
		return nil, err
	}
	step, err := p.Plan(fold.GetLoopStep())
	if err != nil {
		return nil, err
	}
	result, err := p.Plan(fold.GetResult())
	if err != nil {
		return nil, err
	}
	return &evalFold{
		id:        expr.Id,
		accuVar:   fold.AccuVar,
		accu:      accu,
		iterVar:   fold.IterVar,
		iterRange: iterRange,
		cond:      cond,
		step:      step,
		result:    result,
		adapter:   p.adapter,
	}, nil
}

// planConst generates a constant valued Interpretable.
func (p *planner) planConst(expr *exprpb.Expr) (Interpretable, error) {
	val, err := p.constValue(expr.GetConstExpr())
	if err != nil {
		return nil, err
	}
	return NewConstValue(expr.Id, val), nil
}

// constValue converts a proto Constant value to a ref.Val.
func (p *planner) constValue(c *exprpb.Constant) (ref.Val, error) {
	switch c.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		return p.adapter.NativeToValue(c.GetBoolValue()), nil
	case *exprpb.Constant_BytesValue:
		return p.adapter.NativeToValue(c.GetBytesValue()), nil
	case *exprpb.Constant_DoubleValue:
		return p.adapter.NativeToValue(c.GetDoubleValue()), nil
	case *exprpb.Constant_DurationValue:
		return p.adapter.NativeToValue(c.GetDurationValue().AsDuration()), nil
	case *exprpb.Constant_Int64Value:
		return p.adapter.NativeToValue(c.GetInt64Value()), nil
	case *exprpb.Constant_NullValue:
		return p.adapter.NativeToValue(c.GetNullValue()), nil
	case *exprpb.Constant_StringValue:
		return p.adapter.NativeToValue(c.GetStringValue()), nil
	case *exprpb.Constant_TimestampValue:
		return p.adapter.NativeToValue(c.GetTimestampValue().AsTime()), nil
	case *exprpb.Constant_Uint64Value:
		return p.adapter.NativeToValue(c.GetUint64Value()), nil
	}
	return nil, fmt.Errorf("unknown constant type: %v", c)
}

// resolveTypeName takes a qualified string constructed at parse time, applies the proto
// namespace resolution rules to it in a scan over possible matching types in the TypeProvider.
func (p *planner) resolveTypeName(typeName string) (string, bool) {
	for _, qualifiedTypeName := range p.container.ResolveCandidateNames(typeName) {
		if _, found := p.provider.FindType(qualifiedTypeName); found {
			return qualifiedTypeName, true
		}
	}
	return "", false
}

// resolveFunction determines the call target, function name, and overload name from a given Expr
// value.
//
// The resolveFunction resolves ambiguities where a function may either be a receiver-style
// invocation or a qualified global function name.
// - The target expression may only consist of ident and select expressions.
// - The function is declared in the environment using its fully-qualified name.
// - The fully-qualified function name matches the string serialized target value.
func (p *planner) resolveFunction(expr *exprpb.Expr) (*exprpb.Expr, string, string) {
	// Note: similar logic exists within the `checker/checker.go`. If making changes here
	// please consider the impact on checker.go and consolidate implementations or mirror code
	// as appropriate.
	call := expr.GetCallExpr()
	target := call.GetTarget()
	fnName := call.GetFunction()

	// Checked expressions always have a reference map entry, and _should_ have the fully qualified
	// function name as the fnName value.
	oRef, hasOverload := p.refMap[expr.GetId()]
	if hasOverload {
		if len(oRef.GetOverloadId()) == 1 {
			return target, fnName, oRef.GetOverloadId()[0]
		}
		// Note, this namespaced function name will not appear as a fully qualified name in ASTs
		// built and stored before cel-go v0.5.0; however, this functionality did not work at all
		// before the v0.5.0 release.
		return target, fnName, ""
	}

	// Parse-only expressions need to handle the same logic as is normally performed at check time,
	// but with potentially much less information. The only reliable source of information about
	// which functions are configured is the dispatcher.
	if target == nil {
		// If the user has a parse-only expression, then it should have been configured as such in
		// the interpreter dispatcher as it may have been omitted from the checker environment.
		for _, qualifiedName := range p.container.ResolveCandidateNames(fnName) {
			_, found := p.disp.FindOverload(qualifiedName)
			if found {
				return nil, qualifiedName, ""
			}
		}
		// It's possible that the overload was not found, but this situation is accounted for in
		// the planCall phase; however, the leading dot used for denoting fully-qualified
		// namespaced identifiers must be stripped, as all declarations already use fully-qualified
		// names. This stripping behavior is handled automatically by the ResolveCandidateNames
		// call.
		return target, stripLeadingDot(fnName), ""
	}

	// Handle the situation where the function target actually indicates a qualified function name.
	qualifiedPrefix, maybeQualified := p.toQualifiedName(target)
	if maybeQualified {
		maybeQualifiedName := qualifiedPrefix + "." + fnName
		for _, qualifiedName := range p.container.ResolveCandidateNames(maybeQualifiedName) {
			_, found := p.disp.FindOverload(qualifiedName)
			if found {
				// Clear the target to ensure the proper arity is used for finding the
				// implementation.
				return nil, qualifiedName, ""
			}
		}
	}
	// In the default case, the function is exactly as it was advertised: a receiver call on with
	// an expression-based target with the given simple function name.
	return target, fnName, ""
}

func (p *planner) relativeAttr(id int64, eval Interpretable) (InterpretableAttribute, error) {
	eAttr, ok := eval.(InterpretableAttribute)
	if !ok {
		eAttr = &evalAttr{
			adapter: p.adapter,
			attr:    p.attrFactory.RelativeAttribute(id, eval),
		}
	}
	decAttr, err := p.decorate(eAttr, nil)
	if err != nil {
		return nil, err
	}
	eAttr, ok = decAttr.(InterpretableAttribute)
	if !ok {
		return nil, fmt.Errorf("invalid attribute decoration: %v(%T)", decAttr, decAttr)
	}
	return eAttr, nil
}

// toQualifiedName converts an expression AST into a qualified name if possible, with a boolean
// 'found' value that indicates if the conversion is successful.
func (p *planner) toQualifiedName(operand *exprpb.Expr) (string, bool) {
	// If the checker identified the expression as an attribute by the type-checker, then it can't
	// possibly be part of qualified name in a namespace.
	_, isAttr := p.refMap[operand.GetId()]
	if isAttr {
		return "", false
	}
	// Since functions cannot be both namespaced and receiver functions, if the operand is not an
	// qualified variable name, return the (possibly) qualified name given the expressions.
	return containers.ToQualifiedName(operand)
}

func stripLeadingDot(name string) string {
	if strings.HasPrefix(name, ".") {
		return name[1:]
	}
	return name
}
