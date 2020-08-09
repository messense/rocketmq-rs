use std::fmt;

use bitflags::bitflags;
use bitflags_serde_shim::impl_serde_for_bitflags;

bitflags! {
    pub struct Permission: i32 {
        const PRIORITY = 0x1 << 3;
        const READ = 0x1 << 2;
        const WRITE = 0x1 << 1;
        const INHERIT = 0x1 << 0;
    }
}

impl_serde_for_bitflags!(Permission);

impl Permission {
    pub fn is_readable(&self) -> bool {
        *self & Self::READ == Self::READ
    }

    pub fn is_writeable(&self) -> bool {
        *self & Self::WRITE == Self::WRITE
    }

    pub fn is_inherited(&self) -> bool {
        *self & Self::INHERIT == Self::INHERIT
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_readable() {
            write!(f, "R")?;
        } else {
            write!(f, "-")?;
        }
        if self.is_writeable() {
            write!(f, "W")?;
        } else {
            write!(f, "-")?;
        }
        if self.is_inherited() {
            write!(f, "X")
        } else {
            write!(f, "-")
        }
    }
}
