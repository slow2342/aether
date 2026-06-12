use std::collections::HashSet;
use std::sync::Mutex;

use crate::raft::NodeId;

/// Type of alarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum AlarmType {
    /// No alarm.
    None,
    /// Space quota exceeded.
    NoSpace,
    /// Storage corruption detected.
    Corrupt,
}

impl AlarmType {
    /// Convert from proto i32 value.
    pub fn from_proto(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::NoSpace),
            2 => Some(Self::Corrupt),
            _ => None,
        }
    }

    /// Convert to proto i32 value.
    pub fn to_proto(self) -> i32 {
        match self {
            Self::None => 0,
            Self::NoSpace => 1,
            Self::Corrupt => 2,
        }
    }
}

/// Manages cluster-wide alarms.
///
/// Thread-safe: can be shared between the API layer and background tasks.
pub struct AlarmManager {
    inner: Mutex<AlarmManagerInner>,
}

struct AlarmManagerInner {
    /// Active alarms indexed by (member_id, alarm_type).
    alarms: HashSet<(NodeId, AlarmType)>,
}

impl AlarmManager {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(AlarmManagerInner {
                alarms: HashSet::new(),
            }),
        }
    }

    /// Activate an alarm for a member. Returns `true` if this is a new alarm.
    pub fn activate(&self, member_id: NodeId, alarm_type: AlarmType) -> bool {
        if alarm_type == AlarmType::None {
            return false;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.alarms.insert((member_id, alarm_type))
    }

    /// Acknowledge (dismiss) an alarm for a member.
    ///
    /// If `member_id` is 0, acknowledges the alarm for all members.
    pub fn acknowledge(&self, member_id: NodeId, alarm_type: AlarmType) {
        if alarm_type == AlarmType::None {
            return;
        }
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if member_id == 0 {
            inner.alarms.retain(|(_, a)| *a != alarm_type);
        } else {
            inner.alarms.remove(&(member_id, alarm_type));
        }
    }

    /// Get all active alarms as (member_id, alarm_type) pairs.
    pub fn get_all(&self) -> Vec<(NodeId, AlarmType)> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.alarms.iter().copied().collect()
    }

    /// Check if a specific alarm is active for a member.
    pub fn has_alarm(&self, member_id: NodeId, alarm_type: AlarmType) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.alarms.contains(&(member_id, alarm_type))
    }
}

impl Default for AlarmManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activate_and_get() {
        let mgr = AlarmManager::new();
        assert!(mgr.activate(1, AlarmType::NoSpace));
        assert!(!mgr.activate(1, AlarmType::NoSpace)); // duplicate

        let alarms = mgr.get_all();
        assert_eq!(alarms.len(), 1);
        assert_eq!(alarms[0].0, 1);
        assert_eq!(alarms[0].1, AlarmType::NoSpace);
    }

    #[test]
    fn test_activate_ignores_none() {
        let mgr = AlarmManager::new();
        assert!(!mgr.activate(1, AlarmType::None));
        assert!(mgr.get_all().is_empty());
    }

    #[test]
    fn test_acknowledge_specific_member() {
        let mgr = AlarmManager::new();
        mgr.activate(1, AlarmType::NoSpace);
        mgr.activate(2, AlarmType::NoSpace);

        mgr.acknowledge(1, AlarmType::NoSpace);
        assert!(!mgr.has_alarm(1, AlarmType::NoSpace));
        assert!(mgr.has_alarm(2, AlarmType::NoSpace));
    }

    #[test]
    fn test_acknowledge_all_members() {
        let mgr = AlarmManager::new();
        mgr.activate(1, AlarmType::NoSpace);
        mgr.activate(2, AlarmType::NoSpace);
        mgr.activate(3, AlarmType::Corrupt);

        mgr.acknowledge(0, AlarmType::NoSpace);
        assert!(mgr.get_all().len() == 1);
        assert!(mgr.has_alarm(3, AlarmType::Corrupt));
    }

    #[test]
    fn test_acknowledge_ignores_none() {
        let mgr = AlarmManager::new();
        mgr.activate(1, AlarmType::NoSpace);
        mgr.acknowledge(1, AlarmType::None);
        assert!(mgr.has_alarm(1, AlarmType::NoSpace));
    }

    #[test]
    fn test_multiple_alarm_types() {
        let mgr = AlarmManager::new();
        mgr.activate(1, AlarmType::NoSpace);
        mgr.activate(1, AlarmType::Corrupt);

        assert_eq!(mgr.get_all().len(), 2);
        assert!(mgr.has_alarm(1, AlarmType::NoSpace));
        assert!(mgr.has_alarm(1, AlarmType::Corrupt));
    }

    #[test]
    fn test_alarm_type_proto_roundtrip() {
        for val in [0i32, 1, 2] {
            let alarm = AlarmType::from_proto(val).unwrap();
            assert_eq!(alarm.to_proto(), val);
        }
        assert!(AlarmType::from_proto(99).is_none());
    }
}
