//! The project mascot: 小运 the Canal Crab.
//!
//! Lives in `canal-common` because it is the one crate both the CLI and the
//! admin server already depend on — `canal-cli` depends on `canal-admin`, so
//! the pet cannot live in either without a cycle.
//!
//! The artwork has a single source of truth: `docs/assets/canal-pet.svg` is
//! embedded at compile time, so editing the vector file updates the running
//! server too (Cargo tracks the file and rebuilds this crate).

/// Mascot name, for greetings and page titles.
pub const PET_NAME: &str = "小运 (Canal Crab)";

/// One-line description of what the mascot is doing.
pub const PET_TAGLINE: &str = "keeper of the lock on the data canal";

/// Terminal-sized ASCII reduction of `docs/assets/canal-pet.svg`:
/// the crab on the lock wall, one claw on the paddle wheel, the canal below.
pub const CANAL_CRAB: &str = r#"     \    \              /    /
      \    \____________/    /
    ___\_                  _/___
   /     o                o     \
  |               __              |
   \          \________/         /
    '.__________________________.'
   __/   /     |      |     \   \__
  |==================================|
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"#;

/// The vector original, embedded. Served by the admin API at `/pet.svg`.
pub const PET_SVG: &str = include_str!("../../../docs/assets/canal-pet.svg");

/// A short banner combining the art with the project line, for CLI output.
pub fn banner() -> String {
    format!("{CANAL_CRAB}\n\n{PET_NAME} — {PET_TAGLINE}\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_mascot_is_printable_and_has_the_canal() {
        assert!(CANAL_CRAB.lines().count() >= 8, "the art lost its rows");
        // Eyes, the lock wall and the water are what make it this mascot and
        // not just any ASCII blob — pin them so a careless edit is caught.
        assert!(CANAL_CRAB.contains('o'), "the crab lost its eyes");
        assert!(CANAL_CRAB.contains("=="), "the lock wall is missing");
        assert!(CANAL_CRAB.contains('~'), "the canal is missing");
        assert!(
            CANAL_CRAB.lines().all(|l| !l.ends_with(' ')),
            "trailing spaces would break terminal alignment"
        );
    }

    #[test]
    fn embedded_svg_is_the_real_artwork() {
        assert!(PET_SVG.starts_with("<svg"), "embedded file is not SVG");
        assert!(PET_SVG.contains("</svg>"), "embedded SVG is truncated");
        // The vector original is the crab; make sure we embedded the right file.
        assert!(PET_SVG.contains("viewBox"), "missing viewBox");
        assert!(PET_SVG.len() > 4_000, "suspiciously small for the artwork");
    }

    #[test]
    fn banner_contains_art_and_name() {
        let b = banner();
        assert!(b.contains(CANAL_CRAB));
        assert!(b.contains(PET_NAME));
    }
}
