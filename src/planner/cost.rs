// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

//! Cost functions to extract the best plan.

use egg::Language;
use tracing::debug;
use crate::catalog::*;

use super::*;

/// The main cost function.
pub struct CostFn<'a> {
    pub egraph: &'a EGraph,
    pub catalog: &'a RootCatalog,
}

impl CostFn<'_> {

    pub fn column_is_required(&mut self, index: &ColumnRefId) -> bool{
        self.catalog.get_column(index).unwrap().is_required()
    }


    pub fn is_constant(&mut self, id:&Id) -> bool{
        let node = &self.egraph[id.clone()].nodes[0];
        match node {
            Expr::Constant(_) | Expr::Type(_) => true,

            Expr::Neg(id) | Expr::Not(id) | Expr::IsNull(id) 
                => self.is_constant(id),
            
            Expr::Sub([lhs,rhs]) | Expr::Add([lhs,rhs]) |
            Expr::Mul([lhs,rhs]) | Expr::Div([lhs,rhs]) | 
            Expr::Mod([lhs,rhs]) | Expr::StringConcat([lhs,rhs]) |
            Expr::Gt([lhs,rhs]) | Expr::Lt([lhs,rhs]) | 
            Expr::GtEq([lhs,rhs]) | Expr::LtEq([lhs,rhs]) | 
            Expr::Eq([lhs,rhs]) | Expr::NotEq([lhs,rhs]) | 
            Expr::And([lhs,rhs]) | Expr::Or([lhs,rhs]) | 
            Expr::Xor([lhs,rhs]) | Expr::Like([lhs,rhs]) | 
            Expr::Extract([lhs,rhs]) | Expr::Cast([lhs,rhs]) 
                => self.is_constant(&lhs) && self.is_constant(&rhs),
            
            Expr::Replace([expr,a,b]) | Expr::Substring([expr,a,b])
                => self.is_constant(&expr) && self.is_constant(&a) && self.is_constant(&b),

            _ => false,
        }
    }

    pub fn cond_check(&mut self, lhs:&Id, rhs:&Id, out: &impl Fn() -> f32) -> f32{
        let lhs_node = &self.egraph[lhs.clone()].nodes[0];
        let rhs_node = &self.egraph[rhs.clone()].nodes[0];

        let mut factor:f32 = 100000.0;
        match lhs_node {
            Expr::Column(idx) => if self.column_is_required(idx) && self.is_constant(rhs){
                factor = 1.0;
            },
            _ => {}
        };
        match rhs_node {
            Expr::Column(_) => factor = 100000.0,
            _ => {}
        }
        println!("factor {}",factor);
        factor //* out()
    }

    pub fn condition_out(&mut self, table:&Id, filter:&Id, out: &impl Fn() -> f32) -> f32{
        let _table_node = &self.egraph[table.clone()].nodes;
        let filter_nodes = &self.egraph[filter.clone()].nodes;

        if filter_nodes.len() == 0 {
            return 100000.0 * out();
        }

        let res = match &filter_nodes[0] {
            Expr::Eq([lhs, rhs]) => self.cond_check(&lhs, &rhs, &out),
            _ => 100000.0 * out(),
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
