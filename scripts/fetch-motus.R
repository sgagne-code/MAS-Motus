library(motus)
library(dplyr)
library(jsonlite)

Sys.setenv(TZ = 'UTC')

motus:::sessionVariable(name = 'userLogin', val = Sys.getenv('MOTUS_USER'))
motus:::sessionVariable(name = 'userPassword', val = Sys.getenv('MOTUS_PASS'))

dir.create('data', showWarnings = FALSE)

recv <- tagme(
  'CTT-V3023D415E26',
  new = FALSE,
  update = TRUE,
  dir = 'data/',
  skipNodes = TRUE
)

metadata(recv)

# Debug: check projs table
projs_debug <- tbl(recv, 'projs') %>% collect()
message("projs table has ", nrow(projs_debug), " rows")
message(paste(capture.output(print(projs_debug)), collapse = "\n"))

tag_deps <- tbl(recv, 'tagDeps') %>%
  collect() %>%
  select(deployID, sex, age)

proj_ids <- tbl(recv, 'projs') %>%
  collect() %>%
  select(id, name)

new_detections <- tbl(recv, 'alltags') %>%
  filter(motusFilter == 1) %>%
  collect() %>%
  group_by(runID) %>%
  slice_min(ts, n = 1) %>%
  ungroup() %>%
  left_join(tag_deps, by = c('tagDeployID' = 'deployID')) %>%
  left_join(proj_ids, by = c('tagProjName' = 'name')) %>%
  select(
    date = ts,
    commonName = speciesEN,
    scientificName = speciesSci,
    tagId = mfgID,
    project = tagProjName,
    projectId = id,
    sex = sex,
    age = age,
    tagDepLat = tagDepLat,
    tagDepLon = tagDepLon
  ) %>%
  mutate(
    date = as.POSIXct(date, origin = '1970-01-01', tz = 'UTC'),
    date = format(date, '%Y-%m-%dT%H:%M:%SZ'),
    id = paste0(tagId, '_', date),
    locationName = NA
  )

existing_path <- 'data/latest_detections.json'

# Load existing cache and detections
existing_index <- list()
if (file.exists(existing_path)) {
  existing_json <- fromJSON(existing_path, flatten = TRUE)
  existing_detections <- as.data.frame(existing_json$detections)
  if (!is.null(existing_json$projectIndex)) {
    existing_index <- as.list(existing_json$projectIndex)
  }
} else {
  existing_detections <- data.frame()
}

# Fill null projectIds from cache
new_detections <- new_detections %>%
  mutate(projectId = ifelse(
    is.na(projectId) & project %in% names(existing_index),
    unlist(existing_index[project]),
    projectId
  ))

# Update cache with any newly resolved IDs
resolved <- new_detections %>%
  filter(!is.na(projectId)) %>%
  distinct(project, projectId)

updated_index <- existing_index
for (i in seq_len(nrow(resolved))) {
  updated_index[[resolved$project[i]]] <- resolved$projectId[i]
}

# Warn about still-null project IDs
nulls <- new_detections %>% filter(is.na(projectId)) %>% distinct(project)
if (nrow(nulls) > 0) {
  message("WARNING: projectId null for: ", paste(nulls$project, collapse = ", "))
  message("Find IDs at https://motus.org and add to projectIndex in latest_detections.json")
}

# Merge with existing detections
if (nrow(existing_detections) > 0) {
  combined <- bind_rows(existing_detections, new_detections) %>%
    distinct(id, .keep_all = TRUE) %>%
    arrange(desc(date))
} else {
  combined <- new_detections %>%
    arrange(desc(date))
}

output <- list(
  updated = format(Sys.time(), '%Y-%m-%dT%H:%M:%SZ', tz = 'UTC'),
  projectIndex = updated_index,
  detections = combined
)

write_json(output, existing_path, pretty = TRUE, auto_unbox = TRUE, na = 'null')
