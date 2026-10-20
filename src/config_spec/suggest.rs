/// Inputs longer than this get no suggestion. Every candidate is a short slug or knob path, so
/// a longer input cannot be within a small edit distance of one, and the bound keeps
/// untrusted input (for example a 1 MiB profile name sent to the Control Plane) cheap.
const MAX_INPUT_BYTES: usize = 256;

/// Returns the candidate closest to `input` by character Levenshtein distance,
/// if that distance is at most `max_distance`. Ties go to the first candidate.
pub(crate) fn closest<'a>(
    input: &str,
    candidates: impl IntoIterator<Item = &'a str>,
    max_distance: usize,
) -> Option<&'a str> {
    if input.len() > MAX_INPUT_BYTES {
        return None;
    }
    let input = input.chars().collect::<Vec<_>>();
    let mut best: Option<(usize, &'a str)> = None;
    for candidate in candidates {
        // Only a strictly closer candidate replaces the best one, so ties keep the first.
        let limit = match best {
            Some((0, _)) => break,
            Some((current, _)) => current - 1,
            None => max_distance,
        };
        if let Some(distance) = levenshtein_within(&input, candidate, limit) {
            best = Some((distance, candidate));
        }
    }
    best.map(|(_, candidate)| candidate)
}

/// The Levenshtein distance between `input` and `candidate` when it is at most `limit`. Gives up
/// early: the distance is at least the length difference, and a row minimum never decreases.
fn levenshtein_within(input: &[char], candidate: &str, limit: usize) -> Option<usize> {
    if input.len().abs_diff(candidate.chars().count()) > limit {
        return None;
    }
    let mut previous = (0..=input.len()).collect::<Vec<_>>();
    let mut current = vec![0; input.len() + 1];
    for (row, other) in candidate.chars().enumerate() {
        current[0] = row + 1;
        for (column, character) in input.iter().enumerate() {
            let substitution = previous[column] + usize::from(*character != other);
            current[column + 1] = substitution
                .min(previous[column + 1] + 1)
                .min(current[column] + 1);
        }
        if current.iter().min().is_some_and(|minimum| *minimum > limit) {
            return None;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    let distance = previous[input.len()];
    (distance <= limit).then_some(distance)
}

#[cfg(test)]
mod tests {
    use super::closest;

    #[test]
    fn closest_matches_within_distance_and_keeps_tie_order() {
        let candidates = ["balanced", "vector", "document", "vectors"];

        assert_eq!(closest("balanced", candidates, 2), Some("balanced"));
        assert_eq!(closest("balancxd", candidates, 2), Some("balanced"));
        assert_eq!(closest("balxxced", candidates, 2), Some("balanced"));
        assert_eq!(closest("balxxxed", candidates, 2), None);
        assert_eq!(closest("blancd", candidates, 2), Some("balanced"));
        assert_eq!(closest("vectorz", candidates, 2), Some("vector"));
        assert_eq!(closest("ab", ["ax", "ay"], 2), Some("ax"));
        assert_eq!(closest("ab", ["ay", "ax"], 2), Some("ay"));
        assert_eq!(closest("anything", [], 2), None);
        assert_eq!(closest("balanc", candidates, 2), Some("balanced"));
        assert_eq!(closest("balan", candidates, 2), None);
    }

    #[test]
    fn long_inputs_get_no_suggestion() {
        let long = "balanced".repeat(1 << 17);
        assert_eq!(closest(&long, ["balanced"], 2), None);
        let padded = format!("{}x", "a".repeat(super::MAX_INPUT_BYTES));
        assert_eq!(closest(&padded, [padded.as_str()], 2), None);
        let fits = "a".repeat(super::MAX_INPUT_BYTES);
        assert_eq!(closest(&fits, [fits.as_str()], 2), Some(fits.as_str()));
    }
}
