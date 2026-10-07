from ml.guards import SplitCounts, Thresholds, assess

ENOUGH = SplitCounts(
    train_samples=1000,
    train_positives=100,
    test_samples=400,
    test_positives=60,
    test_onsets=40,
    test_negatives=340,
)


def test_enough_data_is_reliable():
    result = assess(ENOUGH)
    assert result.reliable is True
    assert result.reasons == ()


def test_each_threshold_is_reported_separately():
    for field, value, fragment in [
        ("train_samples", 10, "train samples 10"),
        ("train_positives", 5, "train positives 5"),
        ("test_positives", 5, "test positives 5"),
        ("test_onsets", 2, "test onsets"),
    ]:
        counts = SplitCounts(**{**ENOUGH.__dict__, field: value})
        result = assess(counts)
        assert not result.reliable, field
        assert any(fragment in reason for reason in result.reasons), (field, result.reasons)


def test_a_single_class_test_set_is_never_reliable():
    counts = SplitCounts(**{**ENOUGH.__dict__, "test_negatives": 0})
    assert "single class" in " ".join(assess(counts).reasons)


def test_custom_thresholds_apply():
    small = SplitCounts(
        train_samples=50,
        train_positives=5,
        test_samples=20,
        test_positives=4,
        test_onsets=3,
        test_negatives=16,
    )
    assert not assess(small).reliable
    lenient = Thresholds(
        min_train_samples=10, min_train_positives=1, min_test_positives=1, min_test_onsets=1
    )
    assert assess(small, lenient).reliable


def test_an_empty_history_lists_every_missing_piece():
    nothing = SplitCounts(0, 0, 0, 0, 0, 0)
    assert len(assess(nothing).reasons) >= 4
