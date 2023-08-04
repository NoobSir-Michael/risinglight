// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

//! Cost functions to extract the best plan.

use egg::Language;
use tracing::debug;

use super::*;

/// The main cost function.
pub struct CostFn<'a> {
    pub egraph: &'a EGraph,
}

impl CostFn<'_> {
    pub fn cond_check(&mut self, lhs:&Id, rhs:&Id, out: &impl Fn() -> f32) -> f32{
        let lhs = &self.egraph[lhs.clone()].nodes[0];
        let rhs = &self.egraph[rhs.clone()].nodes[0];
        let mut factor:f32 = 100000.0;
        match lhs {
            Expr::Column(idx) => if idx.column_id == 3 && factor == 100000.0{
                factor = 1.0;
            },
            _ => {}
        };
        match rhs {
            Expr::Column(idx) => if idx.column_id == 3 && factor == 100000.0{
                factor = 1.0;
            },
            _ => {}
        };
        factor
    }

    pub fn condition_out(&mut self, table:&Id, filter:&Id, out: &impl Fn() -> f32) -> f32{
        let table_node = &self.egraph[table.clone()].nodes;
        let filter_nodes = &self.egraph[filter.clone()].nodes;

        // assert!(table_node.len() == 1 && filter_nodes.len() == 1, "size is not 1");
        // match table_node[0] {
        //     Expr::Table(t) => println!("tableId: {}, {}",t.schema_id, t.table_id),
        //     _ => println!("err")
        // }
        let res = match &filter_nodes[0] {
            Expr::Eq([lhs, rhs]) => self.cond_check(&lhs, &rhs, &out),
            _ => 100000.0,
        };

        return res;
    }
}

impl egg::CostFunction<Expr> for CostFn<'_> {
    type Cost = f32;
    fn cost<C>(&mut self, enode: &Expr, mut costs: C) -> Self::Cost
    where
        C: FnMut(Id) -> Self::Cost,
    {
        use Expr::*;
        let id = &self.egraph.lookup(enode.clone()).unwrap();
        let mut costs = |i: &Id| costs(*i);
        let rows = |i: &Id| self.egraph[*i].data.rows;
        let cols = |i: &Id| self.egraph[*i].data.schema.len() as f32;
        let nlogn = |x: f32| x * (x + 1.0).log2();
        // The cost of output chunks of a plan.
        let out = || rows(id) * cols(id);

        let c = match enode {
            Scan([table ,_ , filter]) => self.condition_out(table, filter, &out),
            Values(_) => out(),
            Order([_, c]) => nlogn(rows(c)) + out() + costs(c),
            Filter([exprs, c]) => costs(exprs) * rows(c) + out() + costs(c),
            Proj([exprs, c]) | Window([exprs, c]) => costs(exprs) * rows(c) + costs(c),
            Agg([exprs, c]) => costs(exprs) * rows(c) + out() + costs(c),
            HashAgg([exprs, groupby, c]) => {
                ((rows(id) + 1.0).log2() + costs(exprs) + costs(groupby)) * rows(c)
                    + out()
                    + costs(c)
            }
            SortAgg([exprs, groupby, c]) => {
                (costs(exprs) + costs(groupby)) * rows(c) + out() + costs(c)
            }
            Limit([_, _, c]) => out() + costs(c),
            TopN([_, _, _, c]) => (rows(id) + 1.0).log2() * rows(c) + out() + costs(c),
            Join([_, on, l, r]) => costs(on) * rows(l) * rows(r) + out() + costs(l) + costs(r),
            HashJoin([_, _, _, l, r]) => {
                (rows(l) + 1.0).log2() * (rows(l) + rows(r)) + out() + costs(l) + costs(r)
            }
            MergeJoin([_, _, _, l, r]) => out() + costs(l) + costs(r),
            Insert([_, _, c]) | CopyTo([_, c]) => rows(c) * cols(c) + costs(c),
            Empty(_) => 0.0,
            // for expressions, the cost is 0.1x AST size
            Column(_) | Ref(_) => 0.01,
            _ => enode.fold(0.1, |sum, id| sum + costs(&id)),
        };
        debug!(
            "{id}\t{enode:?}\tcost={c}, rows={}, cols={}",
            rows(id),
            cols(id)
        );
        c
    }
}
