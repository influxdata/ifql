package functions

//const IntegralKind = "integral"
//
//type IntegralOpSpec struct {
//	Unit query.Duration `json:"unit"`
//	execute.AggregateConfig
//}
//
//var integralSignature = query.DefaultFunctionSignature()
//
//func init() {
//	integralSignature.Params["unit"] = semantic.Duration
//
//	query.RegisterFunction(IntegralKind, createIntegralOpSpec, integralSignature)
//	query.RegisterOpSpec(IntegralKind, newIntegralOp)
//	plan.RegisterProcedureSpec(IntegralKind, newIntegralProcedure, IntegralKind)
//	execute.RegisterTransformation(IntegralKind, createIntegralTransformation)
//}
//
//func createIntegralOpSpec(args query.Arguments, a *query.Administration) (query.OperationSpec, error) {
//	if err := a.AddParentFromArgs(args); err != nil {
//		return nil, err
//	}
//
//	spec := new(IntegralOpSpec)
//
//	if unit, ok, err := args.GetDuration("unit"); err != nil {
//		return nil, err
//	} else if ok {
//		spec.Unit = unit
//	} else {
//		//Default is 1s
//		spec.Unit = query.Duration(time.Second)
//	}
//
//	if err := spec.AggregateConfig.ReadArgs(args); err != nil {
//		return nil, err
//	}
//	return spec, nil
//}
//
//func newIntegralOp() query.OperationSpec {
//	return new(IntegralOpSpec)
//}
//
//func (s *IntegralOpSpec) Kind() query.OperationKind {
//	return IntegralKind
//}
//
//type IntegralProcedureSpec struct {
//	Unit query.Duration `json:"unit"`
//}
//
//func newIntegralProcedure(qs query.OperationSpec, pa plan.Administration) (plan.ProcedureSpec, error) {
//	spec, ok := qs.(*IntegralOpSpec)
//	if !ok {
//		return nil, fmt.Errorf("invalid spec type %T", qs)
//	}
//
//	return &IntegralProcedureSpec{
//		Unit:            spec.Unit,
//		AggregateConfig: spec.AggregateConfig,
//	}, nil
//}
//
//func (s *IntegralProcedureSpec) Kind() plan.ProcedureKind {
//	return IntegralKind
//}
//func (s *IntegralProcedureSpec) Copy() plan.ProcedureSpec {
//	ns := new(IntegralProcedureSpec)
//	*ns = *s
//
//	ns.AggregateConfig = s.AggregateConfig.Copy()
//
//	return ns
//}
//
//func createIntegralTransformation(id execute.DatasetID, mode execute.AccumulationMode, spec plan.ProcedureSpec, a execute.Administration) (execute.Transformation, execute.Dataset, error) {
//	s, ok := spec.(*IntegralProcedureSpec)
//	if !ok {
//		return nil, nil, fmt.Errorf("invalid spec type %T", spec)
//	}
//	cache := execute.NewBlockBuilderCache(a.Allocator())
//	d := execute.NewDataset(id, mode, cache)
//	t := NewIntegralTransformation(d, cache, s)
//	return t, d, nil
//}
//
//type integralTransformation struct {
//	d     execute.Dataset
//	cache execute.BlockBuilderCache
//
//	spec IntegralProcedureSpec
//}
//
//func NewIntegralTransformation(d execute.Dataset, cache execute.BlockBuilderCache, spec *IntegralProcedureSpec) *integralTransformation {
//	return &integralTransformation{
//		d:     d,
//		cache: cache,
//		spec:  *spec,
//	}
//}
//
//func (t *integralTransformation) RetractBlock(id execute.DatasetID, key execute.PartitionKey) error {
//	return t.d.RetractBlock(key)
//}
//
//func (t *integralTransformation) Process(id execute.DatasetID, b execute.Block) error {
//	builder, new := t.cache.BlockBuilder(b.Key())
//	if !new {
//		return fmt.Errorf("found duplicate block with key: %v", b.Key())
//	}
//	builder.AddCol(execute.ColMeta{
//		Label: t.spec.TimeCol,
//		Type:  execute.TTime,
//	})
//	execute.AddBlockKeyCols(b, builder)
//
//	if err := execute.AppendAggregateTime(t.spec.TimeValue, t.spec.TimeCol, b.Key(), builder); err != nil {
//		return err
//	}
//
//	cols := b.Cols()
//	integrals := make([]*integral, len(cols))
//	for j, c := range cols {
//		found := false
//		for _, label := range t.spec.Columns {
//			if c.Label == label {
//				found = true
//				break
//			}
//		}
//		if found {
//			integrals[j] = newIntegral(t.spec.Unit)
//			builder.AddCol(execute.ColMeta{
//				Label: c.Label,
//				Type:  execute.TFloat,
//			})
//		}
//	}
//
//	err := b.Do(func(cr execute.ColReader) error {
//		for j, c := range builder.Cols() {
//			in := integrals[j]
//			if in == nil {
//				continue
//			}
//			for i, t := range ts {
//				in.updateFloat(t, rr.AtFloat(i, j))
//			}
//		}
//	})
//	if err != nil {
//		return err
//	}
//
//	builder.AppendFloat(valueIdx, t.value())
//
//	for j, in := range integrals {
//		if in == nil {
//			continue
//		}
//		builder.AppendFloat(j, in.value())
//	}
//
//	return nil
//}
//
//func (t *integralTransformation) UpdateWatermark(id execute.DatasetID, mark execute.Time) error {
//	return t.d.UpdateWatermark(mark)
//}
//func (t *integralTransformation) UpdateProcessingTime(id execute.DatasetID, pt execute.Time) error {
//	return t.d.UpdateProcessingTime(pt)
//}
//func (t *integralTransformation) Finish(id execute.DatasetID, err error) {
//	t.d.Finish(err)
//}
//
//func newIntegral(unit time.Duration) *integral {
//	return &integral{
//		first: true,
//		unit:  float64(unit),
//	}
//}
//
//type integral struct {
//	first bool
//	unit  float64
//
//	pFloatValue float64
//	pTime       execute.Time
//
//	sum float64
//}
//
//func (in *integral) value() float64 {
//	return in.sum
//}
//
//func (in *integral) updateFloat(t execute.Time, v float64) {
//	if in.first {
//		in.pTime = t
//		in.pFloatValue = v
//		in.first = false
//		return
//	}
//
//	elapsed := float64(t-in.pTime) / in.unit
//	in.sum += 0.5 * (v + in.pFloatValue) * elapsed
//
//	in.pTime = t
//	in.pFloatValue = v
//}
