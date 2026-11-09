from wikipulse.producer.checkpoint import FileCheckpoint


def test_round_trip(tmp_path):
    cp = FileCheckpoint(tmp_path / "sub" / "cp.json", "https://s/x")
    assert cp.load() is None
    cp.save('[{"topic":"t","partition":0,"timestamp":1}]')
    assert FileCheckpoint(tmp_path / "sub" / "cp.json", "https://s/x").load() == (
        '[{"topic":"t","partition":0,"timestamp":1}]'
    )


def test_ignores_checkpoint_from_another_stream(tmp_path):
    FileCheckpoint(tmp_path / "cp.json", "https://s/a").save("id-1")
    assert FileCheckpoint(tmp_path / "cp.json", "https://s/b").load() is None


def test_corrupt_file_means_no_checkpoint(tmp_path):
    (tmp_path / "cp.json").write_text("{oops")
    assert FileCheckpoint(tmp_path / "cp.json", "https://s/a").load() is None


def test_saving_none_is_a_no_op(tmp_path):
    cp = FileCheckpoint(tmp_path / "cp.json", "https://s/a")
    cp.save(None)
    assert not (tmp_path / "cp.json").exists()
