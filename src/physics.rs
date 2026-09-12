use physics_engine::{
    BodyId, BodyKind, Material, PhysicsError, RigidBody, StepReport, Vec3i, World, WorldConfig,
};

pub const PINNED_PHYSICS_ENGINE_REVISION: &str =
    "8ea513395ad6893e45da7d6fa983b6cd3949a4ac";

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Debug)]
pub struct PhysicsWorldAdapter {
    world: World,
    authoritative_steps: u64,
}

impl PhysicsWorldAdapter {
    pub fn new(config: WorldConfig) -> Self {
        Self {
            world: World::new(config),
            authoritative_steps: 0,
        }
    }

    pub fn authoritative_steps(&self) -> u64 {
        self.authoritative_steps
    }

    pub fn body(&self, id: BodyId) -> Option<&RigidBody> {
        self.world.body(id)
    }

    pub fn add_body(&mut self, body: RigidBody) -> Result<(), PhysicsError> {
        self.world.add_body(body)
    }

    pub fn remove_body(&mut self, id: BodyId) -> Option<RigidBody> {
        self.world.remove_body(id)
    }

    pub fn set_position(&mut self, id: BodyId, position: Vec3i) -> Result<(), PhysicsError> {
        self.world.set_position(id, position)
    }

    pub fn set_velocity(&mut self, id: BodyId, velocity: Vec3i) -> Result<(), PhysicsError> {
        self.world.set_velocity(id, velocity)
    }

    pub fn set_material(&mut self, id: BodyId, material: Material) -> Result<(), PhysicsError> {
        self.world.set_material(id, material)
    }

    pub fn step_authoritative_tick(&mut self) -> Result<StepReport, PhysicsError> {
        let report = self.world.step(1)?;
        self.authoritative_steps = self.authoritative_steps.saturating_add(1);
        Ok(report)
    }

    pub fn state_fingerprint(&self) -> u64 {
        let config = self.world.config();
        let mut hash = FNV_OFFSET_BASIS;
        hash = fnv_update(hash, PINNED_PHYSICS_ENGINE_REVISION.as_bytes());
        hash = fnv_update(hash, &self.authoritative_steps.to_be_bytes());
        hash = hash_vec3(hash, config.gravity);
        hash = fnv_update(hash, &(config.max_events_per_step as u64).to_be_bytes());
        hash = fnv_update(hash, &(config.stabilization_passes as u64).to_be_bytes());
        for body in self.world.bodies() {
            hash = fnv_update(hash, &body.id().0.to_be_bytes());
            hash = fnv_update(
                hash,
                &[match body.kind() {
                    BodyKind::Fixed => 0,
                    BodyKind::Dynamic => 1,
                }],
            );
            hash = hash_vec3(hash, body.position());
            hash = hash_vec3(hash, body.velocity());
            hash = hash_vec3(hash, body.half_extents());
            hash = fnv_update(hash, &body.mass_units().to_be_bytes());
            hash = fnv_update(hash, &body.material().restitution_milli().to_be_bytes());
            hash = fnv_update(hash, &body.material().friction_milli().to_be_bytes());
        }
        hash
    }
}

fn hash_vec3(mut hash: u64, value: Vec3i) -> u64 {
    hash = fnv_update(hash, &value.x.to_be_bytes());
    hash = fnv_update(hash, &value.y.to_be_bytes());
    fnv_update(hash, &value.z.to_be_bytes())
}

fn fnv_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run() -> (u64, Vec3i, u64) {
        let mut adapter = PhysicsWorldAdapter::new(WorldConfig {
            gravity: Vec3i::ZERO,
            ..WorldConfig::default()
        });
        adapter
            .add_body(RigidBody::dynamic(
                BodyId(1),
                Vec3i::ZERO,
                Vec3i::new(3, 0, 0),
                Vec3i::new(1, 1, 1),
            ))
            .unwrap();
        for _ in 0..10 {
            adapter.step_authoritative_tick().unwrap();
        }
        (
            adapter.state_fingerprint(),
            adapter.body(BodyId(1)).unwrap().position(),
            adapter.authoritative_steps(),
        )
    }

    #[test]
    fn authoritative_tick_maps_one_to_one_to_physics_step() {
        let (_, _, steps) = run();
        assert_eq!(steps, 10);
    }

    #[test]
    fn pinned_physics_replay_is_deterministic() {
        assert_eq!(run(), run());
    }
}
