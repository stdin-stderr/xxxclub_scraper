# Graph Report - .  (2026-04-26)

## Corpus Check
- Corpus is ~25,127 words - fits in a single context window. You may not need a graph.

## Summary
- 301 nodes · 589 edges · 16 communities detected
- Extraction: 89% EXTRACTED · 11% INFERRED · 0% AMBIGUOUS · INFERRED: 63 edges (avg confidence: 0.82)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_UI Templates & CSS|UI Templates & CSS]]
- [[_COMMUNITY_Debrid Service Client|Debrid Service Client]]
- [[_COMMUNITY_Web UI & Query Handlers|Web UI & Query Handlers]]
- [[_COMMUNITY_Scene-Torrent DB Operations|Scene-Torrent DB Operations]]
- [[_COMMUNITY_Torrent DB Schema & Init|Torrent DB Schema & Init]]
- [[_COMMUNITY_REST API & Stream Endpoints|REST API & Stream Endpoints]]
- [[_COMMUNITY_Service Orchestration|Service Orchestration]]
- [[_COMMUNITY_Entity Detail API Handlers|Entity Detail API Handlers]]
- [[_COMMUNITY_Torrent Metadata Extraction|Torrent Metadata Extraction]]
- [[_COMMUNITY_Scraper Behaviour Docs|Scraper Behaviour Docs]]
- [[_COMMUNITY_Architecture Overview|Architecture Overview]]
- [[_COMMUNITY_Sites DB Table|Sites DB Table]]
- [[_COMMUNITY_Networks DB Table|Networks DB Table]]
- [[_COMMUNITY_Environment Configuration|Environment Configuration]]
- [[_COMMUNITY_Database Migrations|Database Migrations]]
- [[_COMMUNITY_CSS Design System|CSS Design System]]

## God Nodes (most connected - your core abstractions)
1. `DebridClient` - 21 edges
2. `get_connection()` - 17 edges
3. `cache_get()` - 16 edges
4. `cache_set()` - 16 edges
5. `do_resolve()` - 14 edges
6. `_serial()` - 14 edges
7. `run_once()` - 13 edges
8. `make_key()` - 13 edges
9. `_api_get()` - 13 edges
10. `_render()` - 12 edges

## Surprising Connections (you probably didn't know these)
- `_sync_scene_performers()` --shares_data_with--> `DB Table: scene_performers`  [INFERRED]
  /Users/jollesnaas/Desktop/stdin-stderr/xxxclub_scraper/db.py → db.py
- `_sync_scene_performers()` --shares_data_with--> `DB Table: performers`  [INFERRED]
  /Users/jollesnaas/Desktop/stdin-stderr/xxxclub_scraper/db.py → db.py
- `upsert_scene()` --shares_data_with--> `DB Table: scenes`  [INFERRED]
  /Users/jollesnaas/Desktop/stdin-stderr/xxxclub_scraper/db.py → db.py
- `upsert_torrents()` --shares_data_with--> `DB Table: torrents`  [INFERRED]
  /Users/jollesnaas/Desktop/stdin-stderr/xxxclub_scraper/db.py → db.py
- `score_match()` --shares_data_with--> `DB Table: torrent_meta`  [INFERRED]
  /Users/jollesnaas/Desktop/stdin-stderr/xxxclub_scraper/metadata_fetcher.py → db.py

## Hyperedges (group relationships)
- **Torrent Ingestion Pipeline** — scraper_utils_parse_page, db_upsert_torrents, meta_extract_upsert_torrent_meta [EXTRACTED 0.95]
- **Metadata Enrichment Pipeline (TPDB)** — metadata_fetcher_run_once, metadata_fetcher_porndbclient, db_upsert_scene, db_link_torrent_scene, db_mark_metadata_attempted [EXTRACTED 0.95]
- **Stremio Stream Resolution Flow** — stremio_addon_stream, debrid_debridclient, stremio_addon_do_resolve, debrid_get_stream_url, cache_cache_get [EXTRACTED 0.90]
- **Scene Card → Modal → Stream/Watch Flow** — base_scene_card_styles, scene_modal_template, scene_modal_script_template, stream_template [INFERRED 0.88]
- **Debrid Config Shared via localStorage Across Pages** — stream_localstorage_config, configure_debrid_section, configure_debrid_services_list, stream_debrid_setup [EXTRACTED 0.90]
- **Scene Browse Templates (scenes, movies, site, network, performer all use scene-grid pattern)** — scenes_template, movies_template, site_template, network_template, performer_template, base_scene_card_styles, scene_modal_template, scene_modal_script_template [EXTRACTED 0.95]

## Communities

### Community 0 - "UI Templates & CSS"
Cohesion: 0.07
Nodes (49): Configure Page CSS Component, Debrid Card CSS Component, Hero Section CSS Component, Base HTML Layout Template, Modal Overlay CSS Component, Scene Card CSS Component, Sidebar Navigation Component, Torrent Table CSS Component (+41 more)

### Community 1 - "Debrid Service Client"
Cohesion: 0.09
Nodes (24): DebridAuthError, DebridClient, DebridError, DebridLinkGenerationError, DebridPendingError, _extract_hash_from_magnet(), debrid.DebridClient.get_stream_url, Validate the API key and return account info.          Returns the `data` dict f (+16 more)

### Community 2 - "Web UI & Query Handlers"
Cohesion: 0.06
Nodes (39): _apply_scene_filters(), count_movies(), count_performers(), count_scenes(), count_torrents(), get_network(), get_performer(), get_scene_by_id() (+31 more)

### Community 3 - "Scene-Torrent DB Operations"
Cohesion: 0.09
Nodes (28): fetch_by_hashes(), fetch_unmatched(), link_torrent_scene(), mark_metadata_attempted(), Link a torrent to a scene (idempotent). Updates match_score if re-inserted., Record that metadata lookup was attempted for this torrent., Remove all scene links for a torrent. Returns number of rows deleted., Return torrent + torrent_meta rows for the given info hashes.     Hashes not pre (+20 more)

### Community 4 - "Torrent DB Schema & Init"
Cohesion: 0.1
Nodes (28): init_schema(), known_hashes(), Bulk upsert a list of torrent dicts. Each dict must have at least     info_hash, Return the subset of hashes that already exist in the database., Update seeders/leechers/scraped_at for rows matched by exact title.     Used for, DB Table: torrents, update_counts_by_title(), upsert_torrents() (+20 more)

### Community 5 - "REST API & Stream Endpoints"
Cohesion: 0.14
Nodes (26): api.app (FastAPI REST API), debrid.DebridClient.check_cached, _api_get(), _build_manifest(), catalog(), catalog_with_extras(), configure(), configure_toplevel() (+18 more)

### Community 6 - "Service Orchestration"
Cohesion: 0.21
Nodes (23): debrid.DebridClient.SUPPORTED_SERVICES, entrypoint (main process orchestrator), stremio_addon.router (FastAPI APIRouter), _api_get(), web_ui.app (FastAPI Web UI), configure_ui(), _enrich_scenes(), format_date() (+15 more)

### Community 7 - "Entity Detail API Handlers"
Cohesion: 0.33
Nodes (21): get_network(), get_performer(), get_scene(), get_site(), get_torrent(), list_categories(), list_movies(), list_networks() (+13 more)

### Community 8 - "Torrent Metadata Extraction"
Cohesion: 0.19
Nodes (17): Bulk upsert (info_hash, title, resolution, release_date, sitename) into torrent_, DB Table: torrent_meta, upsert_torrent_meta(), extract_date(), extract_meta(), extract_movie_studio(), extract_movie_title(), extract_resolution() (+9 more)

### Community 10 - "Scraper Behaviour Docs"
Cohesion: 0.67
Nodes (3): Browse Page Scraping Quirks, Top100 Page Scraping Quirks, Page Watcher Poll Cycle Behaviour

### Community 11 - "Architecture Overview"
Cohesion: 1.0
Nodes (2): System Architecture Overview, Web Layer Request Flow

### Community 12 - "Sites DB Table"
Cohesion: 1.0
Nodes (1): DB Table: sites

### Community 13 - "Networks DB Table"
Cohesion: 1.0
Nodes (1): DB Table: networks

### Community 14 - "Environment Configuration"
Cohesion: 1.0
Nodes (1): Environment Variables Configuration

### Community 15 - "Database Migrations"
Cohesion: 1.0
Nodes (1): Database Migration Scripts

### Community 16 - "CSS Design System"
Cohesion: 1.0
Nodes (1): CSS Design System (variables, components)

## Knowledge Gaps
- **84 isolated node(s):** `Return seconds since the most recently scraped row, or None if the table is empt`, `Return the subset of hashes that already exist in the database.`, `Update seeders/leechers/scraped_at for rows matched by exact title.     Used for`, `Return distinct non-null categories sorted alphabetically.`, `Count rows matching the given filters.` (+79 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Architecture Overview`** (2 nodes): `System Architecture Overview`, `Web Layer Request Flow`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Sites DB Table`** (1 nodes): `DB Table: sites`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Networks DB Table`** (1 nodes): `DB Table: networks`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Environment Configuration`** (1 nodes): `Environment Variables Configuration`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Database Migrations`** (1 nodes): `Database Migration Scripts`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `CSS Design System`** (1 nodes): `CSS Design System (variables, components)`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DebridClient` connect `Debrid Service Client` to `REST API & Stream Endpoints`, `Entity Detail API Handlers`?**
  _High betweenness centrality (0.147) - this node is a cross-community bridge._
- **Why does `entrypoint (main process orchestrator)` connect `Service Orchestration` to `Scene-Torrent DB Operations`, `Torrent DB Schema & Init`, `REST API & Stream Endpoints`?**
  _High betweenness centrality (0.116) - this node is a cross-community bridge._
- **Why does `get_connection()` connect `Entity Detail API Handlers` to `Web UI & Query Handlers`, `Scene-Torrent DB Operations`, `Torrent DB Schema & Init`?**
  _High betweenness centrality (0.104) - this node is a cross-community bridge._
- **Are the 13 inferred relationships involving `get_connection()` (e.g. with `list_movies()` and `list_torrents()`) actually correct?**
  _`get_connection()` has 13 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `cache_get()` (e.g. with `list_networks()` and `get_network()`) actually correct?**
  _`cache_get()` has 7 INFERRED edges - model-reasoned connections that need verification._
- **Are the 7 inferred relationships involving `cache_set()` (e.g. with `list_networks()` and `get_network()`) actually correct?**
  _`cache_set()` has 7 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Return seconds since the most recently scraped row, or None if the table is empt`, `Return the subset of hashes that already exist in the database.`, `Update seeders/leechers/scraped_at for rows matched by exact title.     Used for` to the rest of the system?**
  _84 weakly-connected nodes found - possible documentation gaps or missing edges._