/// Returns the candidate closest to `input` by character Levenshtein distance,
/// if that distance is at most `max_distance`. Ties go to the first candidate.
pub(crate) fn closest<'a>(
    input: &str,
    candidates: impl IntoIterator<Item = &'a str>,
    max_distance: usize,
) -> Option<&'a str> {
    let input = input.chars().collect::<Vec<_>>();
    let mut best: Option<(usize, &'a str)> = None;
    for candidate in candidates {
        let distance = levenshtein(&input, candidate);
        if distance <= max_distance && best.is_none_or(|(current, _)| distance < current) {
            best = Some((distance, candidate));
        }
    }
    best.map(|(_, candidate)| candidate)
}

fn levenshtein(input: &[char], candidate: &str) -> usize {
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
        std::mem::swap(&mut previous, &mut current);
    }
    previous[input.len()]
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
    }
}
