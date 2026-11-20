# Operator-owned native workspace

Core44's optional `processLimits.writableStorage` consumes a bounded filesystem;
it does not allocate one. `scripts/native_writable_storage.py` is the Linux host
operator command for that prerequisite. It uses the existing kernel loop/ext4
and system mount manager. It creates no model attention or accounting authority.

Select the base on the host's maintained data filesystem, its existing actor UID
and recorded free-space reserve. Every base ancestor must be root-owned, immutable
to other users and traversable. A private home is not a substitute: the command
refuses inaccessible parents instead of opening them. Root-only images/records
are separate from the actor-owned workspace. Each named image is 32MiB–16GiB,
reserved with `fallocate`; `nodiscard` retains that reservation during formatting.

For regular administrative use, install the qualified script in the existing
root-owned software location and retain its source/hash in the receiving binding.
Invoke that protected copy with Python's isolated mode; an actor-writable checkout
is not the installed operator. `OPERATOR_SCRIPT` below denotes that binding.

One command prepares or restores that exact selection:

```sh
sudo python3 -I -B OPERATOR_SCRIPT prepare \
  --base /srv/operator-workspaces --name reports --uid ACTOR_UID \
  --bytes 33554432 --reserve-bytes HOST_RESERVE_BYTES
```

Replace the UID and reserve from the existing host/body record. The base/name is
the stable operation identity, not a being identifier. Preparation retains one
private descriptor, one image and one static `.mount` unit. The unit is enabled
for normal filesystem startup; no timer, model wakeup or new daemon is installed.
An existing matching filesystem is never reformatted. A changed selection,
unknown file/unit, mutable parent or insufficient host reserve refuses with the
original files retained. Preparation is serialized by the private operator lock.
Each invocation has a 180 second process deadline and 30 second subprocess limits;
timeout retains the descriptor/image for inspection through the same status path.

The returned `root`/`maxBytes` can be captured in the existing private native host
restrictions only after the matching Core/App package and permissions qualify.
This command changes no native profile, tool/root grant, account, lane or goal.
Its status is filesystem readiness, not native execution, model use or adoption.
Core still validates capacity, root identity and the effective write ceiling at
admission and dispatch. Actual boot recovery remains a receiving observation;
an enabled unit alone does not prove that a host reboot succeeded.

Inspect or stop the same selection with the same arguments:

```sh
sudo python3 -I -B OPERATOR_SCRIPT status \
  --base /srv/operator-workspaces --name reports --uid ACTOR_UID \
  --bytes 33554432 --reserve-bytes HOST_RESERVE_BYTES
sudo python3 -I -B OPERATOR_SCRIPT deactivate \
  --base /srv/operator-workspaces --name reports --uid ACTOR_UID \
  --bytes 33554432 --reserve-bytes HOST_RESERVE_BYTES
```

Status does not alter the descriptor. Deactivation performs a normal unmount;
busy work refuses before changing the unit or descriptor. It then stops/disables
only that mount. Neither command deletes retained work or resizes the image.
The same prepare command remounts and enables the original UUID and bytes.
Ordinary project retention operates inside this fixed budget; deactivation leaves
one bounded image, not snapshots or a growing history. Explicit retirement of
the image/data remains with the owning project's actual retention decision.

Root-only structural checks, without mounts or units:

```sh
sudo python3 -B -m unittest discover -s scripts -p test_native_writable_storage.py -v
```

Actual receiving qualification also checks the kernel filesystem as its actor,
busy refusal, retained bytes/UUID across deactivate/prepare, physical backing
reservation and the unchanged native/profile boundary. The disposable native
raw/V8 storage journey remains in `scripts/qualify_native_writable_storage.py`.
