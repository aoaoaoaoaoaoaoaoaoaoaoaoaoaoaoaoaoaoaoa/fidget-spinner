use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use camino::Utf8PathBuf;
use fidget_spinner_core::Slug;
use fidget_spinner_store_sqlite::{FrontierChartScene, StoreError};

const CHART_SCENE_CACHE_CAPACITY: usize = 8;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::ui) struct ChartSceneCacheKey {
    project_root: Utf8PathBuf,
    frontier_slug: Slug,
    refresh_token: String,
}

impl ChartSceneCacheKey {
    pub(in crate::ui) fn new(
        project_root: Utf8PathBuf,
        frontier_slug: Slug,
        refresh_token: String,
    ) -> Self {
        Self {
            project_root,
            frontier_slug,
            refresh_token,
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct ChartSceneCache {
    entries: VecDeque<(ChartSceneCacheKey, Arc<FrontierChartScene>)>,
}

impl ChartSceneCache {
    fn get(&mut self, key: &ChartSceneCacheKey) -> Option<Arc<FrontierChartScene>> {
        let index = self
            .entries
            .iter()
            .position(|(candidate, _)| candidate == key)?;
        let entry = self.entries.remove(index)?;
        let scene = Arc::clone(&entry.1);
        self.entries.push_front(entry);
        Some(scene)
    }

    fn insert(&mut self, key: ChartSceneCacheKey, scene: Arc<FrontierChartScene>) {
        self.entries.retain(|(candidate, _)| candidate != &key);
        self.entries.push_front((key, scene));
        self.entries.truncate(CHART_SCENE_CACHE_CAPACITY);
    }
}

#[derive(Clone, Debug, Default)]
pub(in crate::ui) struct SharedChartSceneCache(Arc<Mutex<ChartSceneCache>>);

impl SharedChartSceneCache {
    pub(in crate::ui) fn get(
        &self,
        key: &ChartSceneCacheKey,
    ) -> Result<Option<Arc<FrontierChartScene>>, StoreError> {
        self.0
            .lock()
            .map_err(|_| StoreError::InvalidInput("chart scene cache lock is poisoned".to_owned()))
            .map(|mut cache| cache.get(key))
    }

    pub(in crate::ui) fn insert(
        &self,
        key: ChartSceneCacheKey,
        scene: Arc<FrontierChartScene>,
    ) -> Result<(), StoreError> {
        self.0
            .lock()
            .map_err(|_| StoreError::InvalidInput("chart scene cache lock is poisoned".to_owned()))
            .map(|mut cache| cache.insert(key, scene))
    }
}
