#[derive(Clone, Debug)]
pub struct ScanPlan {
    mode: ScanMode,
}

#[derive(Clone, Debug)]
pub enum ScanMode {
    Head { rows: usize },
    Tail { rows: usize },
}

impl ScanPlan {
    pub fn head(rows: usize) -> Self {
        Self {
            mode: ScanMode::Head { rows },
        }
    }

    pub fn tail(rows: usize) -> Self {
        Self {
            mode: ScanMode::Tail { rows },
        }
    }

    pub fn mode(&self) -> &ScanMode {
        &self.mode
    }
}
