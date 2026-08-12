"""Helpers for building resource names that fit downstream length limits.

Some AWS names are consumed by machinery far downstream of where we construct them,
and that machinery has limits we do not control. The motivating case:

An AWS Batch job definition name becomes the ECS task family, which the ECS agent
embeds in the docker volume name it generates for each mounted volume::

    ecs-<task family>-<revision>-<volume name>-<20 hex>

For an EFS volume mounted with transit encryption, ``amazon-efs-utils`` then derives
its per-mount TLS state directory from that volume name, and runs ``openssl ca``
inside it. ``openssl`` guards the CA database path against a hard-coded 256-byte
stack buffer (``BSIZE`` in ``apps/lib/apps.c``)::

    /var/run/efs/<state dir>/database/index.txt        must be <= 246 chars

Working backwards through that chain gives the budget in
:data:`ECS_VOLUME_COMPONENT_BUDGET`. Exceeding it does not fail at registration --
it fails minutes later when the container tries to start, as::

    CannotStartContainerError: ... Failed to create self-signed client-side
    certificate ... File name too long

which points at none of the code that actually chose the name. These helpers keep
names inside the budget, and :func:`check_ecs_volume_component_budget` turns an
overrun into an immediate, legible error instead.
"""

__all__ = [
    "ECS_VOLUME_COMPONENT_BUDGET",
    "DEFAULT_HASH_LENGTH",
    "EFS_VOLUME_NAME_MAX_LENGTH",
    "condense_str",
    "build_efs_volume_name",
    "check_ecs_volume_component_budget",
]

from pathlib import Path

from aibs_informatics_core.utils.hashing import sha256_hexdigest

#: Combined budget, in characters, for ``task family + revision + volume name``.
#:
#: Derived from the chain in the module docstring::
#:
#:     openssl BSIZE guard      strlen(dbfile) + len("new") + 6 < 256  -> dbfile <= 246
#:     dbfile                   "/var/run/efs/" (13) + state_dir + "/database/index.txt" (19)
#:                                                                  -> state_dir <= 214
#:     state_dir                fs id (20) + "." + "var.lib.ecs.volumes." (20)
#:                              + ecs_volume_name + "." + tls port (5) + "+" (1)
#:                                                                  -> ecs_volume_name <= 166
#:     ecs_volume_name          "ecs-" (4) + family + "-" + revision + "-" + volume + "-"
#:                              + 20 hex                            -> family+rev+volume <= 139
ECS_VOLUME_COMPONENT_BUDGET = 139

#: Hex characters of hash appended when a value has to be shortened. 8 hex chars is
#: 32 bits: ample when disambiguating a handful of volumes within one job definition.
DEFAULT_HASH_LENGTH = 8

#: Default cap for an EFS volume name. Leaves the bulk of
#: :data:`ECS_VOLUME_COMPONENT_BUDGET` to the job definition name, which carries the
#: caller's execution type and a 64 character hash.
EFS_VOLUME_NAME_MAX_LENGTH = 24


def condense_str(
    value: str,
    max_length: int,
    delimiter: str = "-",
    hash_length: int = DEFAULT_HASH_LENGTH,
) -> str:
    """Shorten ``value`` to at most ``max_length`` characters, preserving uniqueness.

    A value that already fits is returned **unchanged**. That matters: condensing has
    to be a no-op for names already within budget, or adopting this helper would
    silently rename every existing resource.

    A value that does not fit keeps as much of its readable prefix as possible and is
    suffixed with ``delimiter`` plus a hash of the **whole original value**. Hashing
    the original rather than the discarded tail means two values sharing a long common
    prefix still condense to different results.

    The result is deterministic: the same input always condenses to the same output.
    Callers rely on that -- an unstable job definition or volume name would register a
    new revision on every run.

    Args:
        value: The name to condense.
        max_length: Maximum length of the result. Must leave room for the hash suffix
            plus at least one character of prefix.
        delimiter: Separator placed between the truncated prefix and the hash suffix.
        hash_length: Number of hex characters of hash to append.

    Returns:
        ``value`` if it already fits, otherwise ``<prefix><delimiter><hash>``.

    Raises:
        ValueError: If ``hash_length`` is not positive, or if ``max_length`` is too
            small to fit the suffix plus at least one prefix character. Truncating
            without a hash would silently invite collisions, so this is an error
            rather than a fallback.

    Examples:
        >>> condense_str("short-name", max_length=24)
        'short-name'
        >>> condense_str("a" * 40, max_length=24)
        'aaaaaaaaaaaaaaa-e4bcc900'
    """
    if hash_length <= 0:
        raise ValueError(f"hash_length must be positive, got {hash_length}")

    if len(value) <= max_length:
        return value

    suffix_length = len(delimiter) + hash_length
    if max_length <= suffix_length:
        raise ValueError(
            f"Cannot condense to max_length={max_length}: the {suffix_length} character "
            f"suffix (delimiter {delimiter!r} + {hash_length} hash chars) leaves no room "
            f"for a prefix. Raise max_length or lower hash_length."
        )

    prefix = value[: max_length - suffix_length]
    return f"{prefix}{delimiter}{sha256_hexdigest(value)[:hash_length]}"


def build_efs_volume_name(
    mount_path: str | Path,
    file_system_id: str,
    access_point_id: str | None = None,
    max_length: int = EFS_VOLUME_NAME_MAX_LENGTH,
    delimiter: str = "-",
    hash_length: int = DEFAULT_HASH_LENGTH,
) -> str:
    """Build a short, stable, unique ECS volume name for an EFS mount.

    The name leads with the final segment of the mount path -- ``scratch``, ``shared``,
    ``tmp`` -- so a volume is identifiable at a glance in a job definition or a docker
    volume listing. Uniqueness comes from a hash over the full identity (file system,
    access point, and complete mount path) rather than from spelling that identity out,
    which is what made the previous scheme long enough to break EFS mounting.

    Args:
        mount_path: Container path the volume is mounted at.
        file_system_id: EFS file system id.
        access_point_id: EFS access point id, if the mount uses one.
        max_length: Maximum length of the returned name.
        delimiter: Separator between the readable label and the hash.
        hash_length: Number of hex characters of hash to append.

    Returns:
        A name matching ECS's ``[a-zA-Z0-9_-]+`` volume name constraint.

    Examples:
        >>> build_efs_volume_name("/opt/fsap-0acb/scratch", "fs-0326", "fsap-0acb")
        'scratch-7b1de0e2'
    """
    identity = "|".join([file_system_id, access_point_id or "", str(mount_path)])
    label = Path(str(mount_path)).name or file_system_id

    # Sanitize to ECS's allowed character set before measuring, so the length we
    # enforce is the length ECS will actually see.
    label = "".join(c if (c.isalnum() or c in "_-") else "-" for c in label).strip("-")
    if not label:
        label = "vol"

    suffix = f"{delimiter}{sha256_hexdigest(identity)[:hash_length]}"
    return condense_str(
        f"{label}{suffix}",
        max_length=max_length,
        delimiter=delimiter,
        hash_length=hash_length,
    )


def check_ecs_volume_component_budget(
    job_definition_name: str,
    volume_names: list[str],
    max_revision_digits: int = 3,
    budget: int = ECS_VOLUME_COMPONENT_BUDGET,
) -> None:
    """Raise if these names would produce an unmountable EFS volume.

    Fails here -- while the job definition is being built -- rather than minutes later
    with a ``CannotStartContainerError`` from ``efs-utils`` that names none of the
    inputs responsible.

    Args:
        job_definition_name: The Batch job definition name (becomes the ECS task family).
        volume_names: Names of the volumes attached to the job definition.
        max_revision_digits: Digits to reserve for the job definition revision. The
            default of 3 keeps the check valid into the hundreds of revisions; reserving
            too few would let a name pass at revision 9 and fail at revision 10.
        budget: Combined character budget. See :data:`ECS_VOLUME_COMPONENT_BUDGET`.

    Raises:
        ValueError: If the longest volume name combined with the job definition name
            exceeds the budget.
    """
    if not volume_names:
        return
    longest = max(volume_names, key=len)
    total = len(job_definition_name) + max_revision_digits + len(longest)
    if total > budget:
        raise ValueError(
            f"Job definition name and volume names exceed the ECS/EFS mount path budget: "
            f"job_definition_name({len(job_definition_name)}) + "
            f"revision({max_revision_digits}) + volume({len(longest)}) = {total} > {budget}. "
            f"Exceeding it fails at container start with a 'File name too long' error from "
            f"efs-utils, not at registration. "
            f"job_definition_name={job_definition_name!r}, longest volume={longest!r}."
        )
