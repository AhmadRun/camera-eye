//! Turn-by-turn steps from an OSRM route, in plain English (port of the
//! server half of `src/data/routeSteps.js`). The distance and duration
//! formatters in that file are browser-only and are not ported.

use serde_json::{Map, Value, json};

use super::super::js_value::{get, round, to_number};

/// Cap on steps returned for one route.
pub const ROUTE_STEPS_MAX: usize = 200;

fn clean_text(value: Option<&Value>) -> String {
    value
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn direction_word(key: &str) -> &str {
    match key {
        "left" => "left",
        "right" => "right",
        "slight left" => "slightly left",
        "slight right" => "slightly right",
        "sharp left" => "sharply left",
        "sharp right" => "sharply right",
        "straight" => "straight",
        "uturn" => "around",
        other => other,
    }
}

/// `roadLabel`: "Name (Ref)", "Name", "Ref", or "".
pub fn road_label(step: Option<&Value>) -> String {
    let name = clean_text(get(step, "name"));
    let reference = clean_text(get(step, "ref"));
    if !name.is_empty() && !reference.is_empty() && !name.contains(&reference) {
        return format!("{name} ({reference})");
    }
    if name.is_empty() { reference } else { name }
}

fn ordinal(n: Option<&Value>) -> Option<String> {
    let n = n.and_then(Value::as_f64)?;
    if n.fract() != 0.0 || n < 1.0 || !n.is_finite() {
        return None;
    }
    let n = n as u64;
    let suffix = if (11..=13).contains(&(n % 100)) {
        "th"
    } else {
        match n % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    Some(format!("{n}{suffix}"))
}

/// `instructionFor`: one English sentence for an OSRM step.
pub fn instruction_for(step: Option<&Value>) -> String {
    let kind = clean_text(get(step, "type")).to_lowercase();
    let modifier = clean_text(get(step, "modifier")).to_lowercase();
    let dir = direction_word(&modifier).to_string();
    let road = road_label(step);
    let onto = if road.is_empty() {
        String::new()
    } else {
        format!(" onto {road}")
    };
    match kind.as_str() {
        "depart" => {
            if road.is_empty() {
                "Head out".into()
            } else {
                format!("Head out on {road}")
            }
        }
        "arrive" => {
            if modifier == "left" || modifier == "right" {
                format!("Arrive at your destination, on the {modifier}")
            } else {
                "Arrive at your destination".into()
            }
        }
        "turn" => {
            if modifier == "uturn" {
                format!("Make a U-turn{onto}")
            } else if modifier == "straight" {
                format!("Continue straight{onto}")
            } else if !dir.is_empty() {
                format!("Turn {dir}{onto}")
            } else {
                format!("Turn{onto}")
            }
        }
        "new name" => format!("Continue{onto}"),
        "continue" => {
            if modifier == "uturn" {
                format!("Make a U-turn{onto}")
            } else if !dir.is_empty() && dir != "straight" {
                format!("Continue {dir}{onto}")
            } else {
                format!("Continue straight{onto}")
            }
        }
        "end of road" => {
            if dir.is_empty() {
                format!("At the end of the road, continue{onto}")
            } else {
                format!("At the end of the road, turn {dir}{onto}")
            }
        }
        "fork" => {
            if dir.is_empty() {
                format!("Continue at the fork{onto}")
            } else {
                format!("Keep {dir} at the fork{onto}")
            }
        }
        "merge" => {
            if dir.is_empty() {
                format!("Merge{onto}")
            } else {
                format!("Merge {dir}{onto}")
            }
        }
        "on ramp" => {
            if dir.is_empty() {
                format!("Take the ramp{onto}")
            } else {
                format!("Take the ramp on the {dir}{onto}")
            }
        }
        "off ramp" => {
            if dir.is_empty() {
                format!("Take the exit{onto}")
            } else {
                format!("Take the exit on the {dir}{onto}")
            }
        }
        "roundabout" | "rotary" => match ordinal(get(step, "exit")) {
            Some(exit) => format!("At the roundabout, take the {exit} exit{onto}"),
            None => format!("Enter the roundabout{onto}"),
        },
        "roundabout turn" => {
            if dir.is_empty() {
                format!("At the roundabout, continue{onto}")
            } else {
                format!("At the roundabout, turn {dir}{onto}")
            }
        }
        "exit roundabout" | "exit rotary" => format!("Exit the roundabout{onto}"),
        "use lane" => {
            if dir.is_empty() {
                format!("Continue{onto}")
            } else {
                format!("Use the {dir} lane{onto}")
            }
        }
        _ => format!("Continue{onto}"),
    }
}

/// `Number(x.toFixed(6))`.
fn fixed6(value: f64) -> f64 {
    format!("{value:.6}").parse().unwrap_or(value)
}

/// One normalized step, in the JS key order.
struct Step {
    index: usize,
    kind: String,
    modifier: Option<String>,
    exit: Option<Value>,
    name: String,
    reference: Option<String>,
    distance_m: f64,
    duration_s: f64,
    lon: f64,
    lat: f64,
    instruction: String,
}

impl Step {
    fn as_value(&self) -> Value {
        use super::super::js_value::number_value;
        json!({
            "index": self.index,
            "type": self.kind,
            "modifier": self.modifier,
            "exit": self.exit.clone().unwrap_or(Value::Null),
            "name": self.name,
            "ref": self.reference,
            "distanceM": number_value(self.distance_m),
            "durationS": number_value(self.duration_s),
            "lon": number_value(self.lon),
            "lat": number_value(self.lat),
            "instruction": self.instruction,
        })
    }

    /// The `{type, modifier, exit, name, ref}` view `instructionFor` reads.
    fn label_view(&self) -> Value {
        let mut map = Map::new();
        map.insert("type".into(), Value::String(self.kind.clone()));
        if let Some(modifier) = &self.modifier {
            map.insert("modifier".into(), Value::String(modifier.clone()));
        }
        if let Some(exit) = &self.exit {
            map.insert("exit".into(), exit.clone());
        }
        map.insert("name".into(), Value::String(self.name.clone()));
        if let Some(reference) = &self.reference {
            map.insert("ref".into(), Value::String(reference.clone()));
        }
        Value::Object(map)
    }
}

/// `normalizeOsrmSteps`: flatten legs, fold roundabout exits, cap the list.
/// Returns the steps as JSON and whether the list was cut off.
pub fn normalize_osrm_steps(route: Option<&Value>) -> (Vec<Value>, bool) {
    let mut out: Vec<Step> = Vec::new();
    let legs = get(route, "legs").and_then(Value::as_array);
    for leg in legs.into_iter().flatten() {
        let steps = get(Some(leg), "steps").and_then(Value::as_array);
        for raw in steps.into_iter().flatten() {
            let maneuver = get(Some(raw), "maneuver");
            let location = get(maneuver, "location").and_then(Value::as_array);
            let lon = to_number(location.and_then(|l| l.first()));
            let lat = to_number(location.and_then(|l| l.get(1)));
            if !lon.is_finite() || !lat.is_finite() {
                continue;
            }
            let kind = {
                let text = clean_text(get(maneuver, "type")).to_lowercase();
                if text.is_empty() {
                    "continue".to_string()
                } else {
                    text
                }
            };
            let modifier = clean_text(get(maneuver, "modifier")).to_lowercase();
            let exit = get(maneuver, "exit")
                .filter(|e| {
                    e.as_f64()
                        .is_some_and(|n| n.is_finite() && n.fract() == 0.0)
                })
                .cloned();
            let reference = clean_text(get(Some(raw), "ref"));
            let positive = |v: f64| if v.is_nan() || v == 0.0 { 0.0 } else { v };
            let step = Step {
                index: out.len(),
                kind: kind.clone(),
                modifier: (!modifier.is_empty()).then_some(modifier),
                exit,
                name: clean_text(get(Some(raw), "name")),
                reference: (!reference.is_empty()).then_some(reference),
                distance_m: round(positive(to_number(get(Some(raw), "distance")))).max(0.0),
                duration_s: round(positive(to_number(get(Some(raw), "duration")))).max(0.0),
                lon: fixed6(lon),
                lat: fixed6(lat),
                instruction: String::new(),
            };
            if (kind == "exit roundabout" || kind == "exit rotary")
                && out.last().is_some_and(|previous| {
                    matches!(
                        previous.kind.as_str(),
                        "roundabout" | "rotary" | "roundabout turn"
                    )
                })
            {
                let previous = out.last_mut().expect("checked above");
                previous.distance_m += step.distance_m;
                previous.duration_s += step.duration_s;
                let previous_label = road_label(Some(&previous.label_view()));
                let step_label = road_label(Some(&step.label_view()));
                if previous_label.is_empty() && !step_label.is_empty() {
                    previous.name = step.name.clone();
                    previous.reference = step.reference.clone();
                    previous.instruction = instruction_for(Some(&previous.label_view()));
                }
                continue;
            }
            let mut step = step;
            step.instruction = instruction_for(Some(&step.label_view()));
            out.push(step);
            if out.len() >= ROUTE_STEPS_MAX {
                return (out.iter().map(Step::as_value).collect(), true);
            }
        }
    }
    (out.iter().map(Step::as_value).collect(), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn say(step: Value) -> String {
        instruction_for(Some(&step))
    }

    #[test]
    fn instructions_read_like_a_navigator() {
        assert_eq!(
            say(json!({ "type": "depart", "name": "Kaivokatu" })),
            "Head out on Kaivokatu"
        );
        assert_eq!(say(json!({ "type": "depart" })), "Head out");
        assert_eq!(
            say(json!({ "type": "new name", "modifier": "straight", "name": "Simonkatu" })),
            "Continue onto Simonkatu"
        );
        assert_eq!(
            say(json!({ "type": "end of road", "modifier": "left", "name": "Annankatu" })),
            "At the end of the road, turn left onto Annankatu"
        );
        assert_eq!(
            say(json!({ "type": "turn", "modifier": "right", "name": "Kansakoulukatu" })),
            "Turn right onto Kansakoulukatu"
        );
        assert_eq!(
            say(
                json!({ "type": "turn", "modifier": "slight left", "name": "Ring I", "ref": "Kt 50" })
            ),
            "Turn slightly left onto Ring I (Kt 50)"
        );
        assert_eq!(
            say(json!({ "type": "turn", "modifier": "uturn" })),
            "Make a U-turn"
        );
        assert_eq!(
            say(
                json!({ "type": "roundabout", "modifier": "right", "exit": 2, "name": "Malminrinne" })
            ),
            "At the roundabout, take the 2nd exit onto Malminrinne"
        );
        assert_eq!(
            say(json!({ "type": "rotary", "exit": 1 })),
            "At the roundabout, take the 1st exit"
        );
        assert_eq!(
            say(json!({ "type": "roundabout", "exit": 11 })),
            "At the roundabout, take the 11th exit"
        );
        assert_eq!(
            say(json!({ "type": "fork", "modifier": "slight right", "ref": "E12" })),
            "Keep slightly right at the fork onto E12"
        );
        assert_eq!(
            say(json!({ "type": "off ramp", "modifier": "right", "name": "Turunväylä" })),
            "Take the exit on the right onto Turunväylä"
        );
        assert_eq!(
            say(json!({ "type": "on ramp", "modifier": "left" })),
            "Take the ramp on the left"
        );
        assert_eq!(
            say(json!({ "type": "merge", "modifier": "slight left", "name": "Länsiväylä" })),
            "Merge slightly left onto Länsiväylä"
        );
        assert_eq!(
            say(json!({ "type": "arrive", "modifier": "right" })),
            "Arrive at your destination, on the right"
        );
        assert_eq!(
            say(json!({ "type": "arrive" })),
            "Arrive at your destination"
        );
        assert_eq!(
            say(json!({ "type": "made-up", "name": "X" })),
            "Continue onto X"
        );
        assert_eq!(instruction_for(None), "Continue");
    }

    #[test]
    fn road_labels() {
        let label = |v: Value| road_label(Some(&v));
        assert_eq!(
            label(json!({ "name": "Ring I", "ref": "Kt 50" })),
            "Ring I (Kt 50)"
        );
        assert_eq!(
            label(json!({ "name": "E12 Turunväylä", "ref": "E12" })),
            "E12 Turunväylä"
        );
        assert_eq!(label(json!({ "ref": "E12" })), "E12");
        assert_eq!(label(json!({})), "");
    }

    #[test]
    fn normalize_flattens_folds_and_drops() {
        let route = json!({
            "legs": [
                { "steps": [
                    { "maneuver": { "type": "depart", "location": [24.9384, 60.1699] },
                      "name": "Kaivokatu", "distance": 8.3, "duration": 1.3 },
                    { "maneuver": { "type": "roundabout", "modifier": "right", "exit": 2,
                                    "location": [24.9313, 60.1675] },
                      "name": "", "distance": 34.7, "duration": 7.3 },
                    { "maneuver": { "type": "exit roundabout", "modifier": "slight right", "exit": 2,
                                    "location": [24.931, 60.1674] },
                      "name": "Malminrinne", "distance": 100.8, "duration": 17.1 },
                    { "maneuver": { "type": "turn", "modifier": "left" },
                      "name": "Nowhere", "distance": 5, "duration": 1 },
                ] },
                { "steps": [
                    { "maneuver": { "type": "arrive", "modifier": "left",
                                    "location": [24.6559, 60.2055] },
                      "name": "Kirkkojärventie", "distance": 0, "duration": 0 },
                ] },
            ],
        });
        let (steps, truncated) = normalize_osrm_steps(Some(&route));
        assert!(!truncated);
        let kinds: Vec<&str> = steps.iter().map(|s| s["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["depart", "roundabout", "arrive"]);
        assert_eq!(steps[1]["distanceM"], 136);
        assert_eq!(steps[1]["durationS"], 24);
        assert_eq!(
            steps[1]["instruction"],
            "At the roundabout, take the 2nd exit onto Malminrinne"
        );
        let indexes: Vec<u64> = steps.iter().map(|s| s["index"].as_u64().unwrap()).collect();
        assert_eq!(indexes, [0, 1, 2]);
        assert_eq!(
            steps[2]["instruction"],
            "Arrive at your destination, on the left"
        );
        assert_eq!(steps[0]["lon"], 24.9384);
        assert_eq!(steps[0]["lat"], 60.1699);
        assert_eq!(steps[0]["modifier"], Value::Null);
        assert_eq!(steps[0]["ref"], Value::Null);
        assert_eq!(normalize_osrm_steps(None), (vec![], false));
        assert_eq!(
            normalize_osrm_steps(Some(&json!({ "legs": [{ "steps": null }] }))),
            (vec![], false)
        );
    }

    #[test]
    fn the_step_list_is_capped_and_says_so() {
        let many: Vec<Value> = (0..ROUTE_STEPS_MAX + 50)
            .map(|i| {
                json!({
                    "maneuver": { "type": "turn", "modifier": "left", "location": [i as f64 * 0.001, 0.5] },
                    "name": format!("Street {i}"),
                    "distance": 10,
                    "duration": 2,
                })
            })
            .collect();
        let (steps, truncated) =
            normalize_osrm_steps(Some(&json!({ "legs": [{ "steps": many }] })));
        assert_eq!(steps.len(), ROUTE_STEPS_MAX);
        assert!(truncated);
        let fits = json!({ "legs": [{ "steps": many[..ROUTE_STEPS_MAX - 1] }] });
        let (steps, truncated) = normalize_osrm_steps(Some(&fits));
        assert_eq!(steps.len(), ROUTE_STEPS_MAX - 1);
        assert!(!truncated);
    }
}
