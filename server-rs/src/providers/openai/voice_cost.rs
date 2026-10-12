//! The voice model registry from `src/voice/voiceCost.js`: the part the token
//! endpoint uses. Only this module maps a tier NAME to an OpenAI model id, so
//! an unknown or hostile `?tier=` never reaches OpenAI as a model id.
//!
//! The rest of voiceCost.js (rate tables, the spend tracker) runs in the
//! browser only and is not needed by the server.

/// One entry of `VOICE_MODELS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoiceModel {
    pub tier: &'static str,
    pub id: &'static str,
}

pub const STANDARD: VoiceModel = VoiceModel {
    tier: "standard",
    id: "gpt-realtime-2",
};

pub const MINI: VoiceModel = VoiceModel {
    tier: "mini",
    id: "gpt-realtime-2.1-mini",
};

/// `VOICE_MODELS`, in registry order.
pub const VOICE_MODELS: [VoiceModel; 2] = [STANDARD, MINI];

/// `DEFAULT_VOICE_TIER`.
pub const DEFAULT_VOICE_TIER: VoiceModel = STANDARD;

/// `isKnownVoiceTier`: true only for a tier name this build knows.
pub fn is_known_voice_tier(tier: Option<&str>) -> bool {
    lookup(tier).is_some()
}

/// `resolveVoiceModel`: total, falling back to `standard`.
pub fn resolve_voice_model(tier: Option<&str>) -> VoiceModel {
    lookup(tier).unwrap_or(DEFAULT_VOICE_TIER)
}

fn lookup(tier: Option<&str>) -> Option<VoiceModel> {
    let key = tier?.trim().to_lowercase();
    VOICE_MODELS.into_iter().find(|model| model.tier == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiers_resolve_by_name_only() {
        assert_eq!(resolve_voice_model(Some("mini")), MINI);
        assert_eq!(resolve_voice_model(Some("  MINI ")), MINI);
        assert_eq!(resolve_voice_model(Some("standard")), STANDARD);
        for hostile in [
            "",
            "gpt-4o",
            "constructor",
            "__proto__",
            "toString",
            "hasOwnProperty",
        ] {
            assert_eq!(resolve_voice_model(Some(hostile)), STANDARD, "{hostile}");
            assert!(!is_known_voice_tier(Some(hostile)), "{hostile}");
        }
        assert_eq!(resolve_voice_model(None), STANDARD);
        assert!(!is_known_voice_tier(None));
        assert!(is_known_voice_tier(Some("Standard")));
    }

    #[test]
    fn registry_ids_match_the_browser() {
        assert_eq!(STANDARD.id, "gpt-realtime-2");
        assert_eq!(MINI.id, "gpt-realtime-2.1-mini");
        assert_eq!(
            VOICE_MODELS.map(|m| m.tier),
            ["standard", "mini"],
            "VOICE_TIERS order"
        );
    }
}
