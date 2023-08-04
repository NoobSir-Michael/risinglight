use super::*;

pub fn self_def_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(cancel_rules());
    rules
}

#[rustfmt::skip]
fn cancel_rules() -> Vec<Rewrite> { vec![
    // rw!("condition_merge";
    //     "(filter ?cond (scan ?table ?columns null))" =>
    //     "(scan ?table ?columns ?cond)"
    // ),
    // rw!(
    //     "filter_join_union";
    //     "(filter ?c (join ?t ?d ?l ?r))"=>
    //     "(filter (and ?c ?d) (join ?t ?d ?l ?r))"
    // ),
    // rw!(
    //     "and_eq";
    //     "(and (= ?a ?b) (= ?a ?c))" => 
    //     "(and (= ?a ?c) (= ?b ?c))"
    // ),
    // rw!(
    //     "duplicate_filter";
    //     "(filter ?e ?c)" => "(filter ?e (filter ?e ?c))"
    // )
]}