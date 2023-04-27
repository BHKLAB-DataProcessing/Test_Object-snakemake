library(PharmacoGx)

options(stringsAsFactors = FALSE)
args <- commandArgs(trailingOnly = TRUE)
processed_dir <- paste0(args[[1]], "processed")

# processed_dir <- "/Users/minoru/Code/bhklab/DataProcessing/PSet/PSet_FIMM-snakemake/processed"

unzip(file.path(processed_dir, "raw_sense_slices.zip"), exdir = file.path(processed_dir, "slices"), junkpaths = TRUE)

files <- list.files(file.path(processed_dir, "slices"), full.names = TRUE)
dir.create(file.path(processed_dir, "slices_recomp"))
for (file in files) {
  print(file)
  mybasenm <- basename(file)

  slice <- readRDS(file)

  res <- PharmacoGx:::.calculateFromRaw(slice)

  saveRDS(res, file = file.path(processed_dir, "slices_recomp", gsub(mybasenm, pattern = ".rds", replacement = "_recomp.rds", fixed = TRUE)))
}

recalc_files <- list.files(path = file.path(processed_dir, "slices_recomp"), full.names = TRUE)
slices <- list()

for (fn in recalc_files) {
  temp <- readRDS(fn)
  parTable <- do.call(rbind, temp[[3]])
  # print(head(rownames(parTable)))
  # print(str(temp[[3]]))
  n <- cbind(
    "aac_recomputed" = as.numeric(unlist(temp[[1]])) / 100,
    "ic50_recomputed" = as.numeric(unlist(temp[[2]])),
    "HS" = as.numeric(unlist(parTable[, 1])),
    "E_inf" = as.numeric(unlist(parTable[, 2])),
    "EC50" = as.numeric(unlist(parTable[, 3]))
  )
  print(head(rownames(n)))
  rownames(n) <- names(temp[[3]])
  slices[[fn]] <- n
}

res <- do.call(rbind, slices)

save(res, file = file.path(processed_dir, "profiles.RData"))

unlink(file.path(processed_dir, 'slices'), recursive=TRUE)
unlink(file.path(processed_dir, 'slices_recomp'), recursive=TRUE)