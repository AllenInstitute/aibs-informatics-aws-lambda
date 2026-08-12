import pytest

from aibs_informatics_aws_lambda.handlers.demand.naming import (
    ECS_VOLUME_COMPONENT_BUDGET,
    build_efs_volume_name,
    check_ecs_volume_component_budget,
)


class TestBuildEfsVolumeName:
    FS = "fs-0326ec44ed80ab9fd"
    AP_SCRATCH = "fsap-0acb9f234e1b57786"
    AP_SHARED = "fsap-08549dedf6d93164b"

    def test__leads_with_the_mount_path_basename(self):
        name = build_efs_volume_name(
            "/opt/fsap-0acb9f234e1b57786/scratch", self.FS, self.AP_SCRATCH
        )
        assert name.startswith("scratch-")

    def test__fits_the_default_budget(self):
        name = build_efs_volume_name(
            "/opt/fsap-0acb9f234e1b57786/scratch", self.FS, self.AP_SCRATCH
        )
        assert len(name) <= 24

    def test__is_shorter_than_the_scheme_it_replaces(self):
        # The old scheme produced "dev-de-core-opt-fsap-0acb9f234e1b57786-scratch-vol"
        # (50 chars), which is what pushed the mount path over openssl's buffer.
        name = build_efs_volume_name(
            "/opt/fsap-0acb9f234e1b57786/scratch", self.FS, self.AP_SCRATCH
        )
        assert len(name) < 50

    def test__distinct_access_points_yield_distinct_names(self):
        # Both mount at a path ending in the same word in some configurations; the
        # access point must still separate them or ECS would see duplicate volumes.
        a = build_efs_volume_name("/opt/data", self.FS, self.AP_SCRATCH)
        b = build_efs_volume_name("/opt/data", self.FS, self.AP_SHARED)
        assert a != b

    def test__distinct_mount_paths_yield_distinct_names(self):
        a = build_efs_volume_name("/opt/x/scratch", self.FS, self.AP_SCRATCH)
        b = build_efs_volume_name("/opt/y/scratch", self.FS, self.AP_SCRATCH)
        assert a != b

    def test__is_deterministic(self):
        args = ("/opt/fsap-0acb/scratch", self.FS, self.AP_SCRATCH)
        assert build_efs_volume_name(*args) == build_efs_volume_name(*args)

    def test__handles_missing_access_point(self):
        name = build_efs_volume_name("/opt/efs/scratch", self.FS)
        assert name.startswith("scratch-")

    def test__result_is_ecs_safe(self):
        name = build_efs_volume_name("/opt/some path/with.dots", self.FS, self.AP_SCRATCH)
        assert all(c.isalnum() or c in "_-" for c in name), name

    def test__falls_back_when_basename_is_empty(self):
        name = build_efs_volume_name("/", self.FS, self.AP_SCRATCH)
        assert name


class TestCheckEcsVolumeComponentBudget:
    def test__passes_for_realistic_names(self):
        # env_base "dev-de" + 20 char execution_type + 64 char hash -> 92, the exact
        # combination that failed in production with the old 50 char volume names.
        check_ecs_volume_component_budget(
            job_definition_name="dev-de-ocsdv452-filter-test-" + "f" * 64,
            volume_names=["scratch-7b1de0e2", "shared-3a9c1f04"],
        )

    def test__raises_when_over_budget(self):
        with pytest.raises(ValueError, match="exceed the ECS/EFS mount path budget"):
            check_ecs_volume_component_budget(
                job_definition_name="dev-de-" + "x" * 120,
                volume_names=["scratch-7b1de0e2"],
            )

    def test__error_names_the_offending_values(self):
        with pytest.raises(ValueError) as exc:
            check_ecs_volume_component_budget(
                job_definition_name="dev-de-" + "x" * 120,
                volume_names=["a" * 40, "b" * 10],
            )
        message = str(exc.value)
        assert "job_definition_name=" in message
        assert "a" * 40 in message, "should quote the LONGEST volume name"

    def test__measures_against_the_longest_volume_name(self):
        job_definition_name = "d" * (ECS_VOLUME_COMPONENT_BUDGET - 3 - 20)
        check_ecs_volume_component_budget(job_definition_name, volume_names=["v" * 20])
        with pytest.raises(ValueError):
            check_ecs_volume_component_budget(job_definition_name, volume_names=["v" * 21])

    def test__reserves_revision_digits(self):
        # A name that passes at revision 9 must not be allowed to fail at revision 10.
        job_definition_name = "d" * (ECS_VOLUME_COMPONENT_BUDGET - 1 - 20)
        with pytest.raises(ValueError):
            check_ecs_volume_component_budget(job_definition_name, volume_names=["v" * 20])

    def test__no_volumes_is_not_an_error(self):
        check_ecs_volume_component_budget("d" * 200, volume_names=[])
