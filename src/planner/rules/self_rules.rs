use super::*;

pub fn self_def_rules() -> Vec<Rewrite> {
    let mut rules = vec![];
    rules.extend(cancel_rules());
    rules
}

#[rustfmt::skip]
fn cancel_rules() -> Vec<Rewrite> { vec![
    rw!("condition_merge";
        "(filter ?cond (scan ?table ?columns null))" =>
        "(scan ?table ?columns ?cond)"
    ),
]}