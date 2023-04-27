library(PharmacoGx)
library(Biobase)
library(SummarizedExperiment)
# library(biocompute)
library(data.table)
library(parallel)

args <- commandArgs(trailingOnly = TRUE)
download_dir <- paste0(args[[1]], "download")
processed_dir <- paste0(args[[1]], "processed")
out_dir <- args[[1]]
filename <- args[[2]]

standardize <- args[grep("filtered", args)]

standardizeRawDataConcRange <- function(sens.info, sens.raw) {
  unq.drugs <- unique(sens.info$drugid)

  conc.m <- data.table(melt(sens.raw[, , 1], as.is = TRUE))
  conc.m[, drugid := sens.info$drugid[match(Var1, rownames(sens.info))]]
  conc.ranges <- conc.m[, .(l = min(value, na.rm = T), r = max(value, na.rm = T)), c("drugid", "Var1")]
  conc.ranges[, Var1 := NULL]
  conc.ranges <- conc.ranges[, unique(.SD), drugid]
  # conc.ranges[,N := .N, drugid]
  conc.ranges.disj <- conc.ranges[
    ,
    {
      sq <- sort(unique(c(l, r)))
      l <- sq[seq(1, length(sq) - 1)]
      r <- sq[seq(2, length(sq))]
      .(l = l, r = r)
    },
    drugid
  ]
  ## Function below returns all consecutive ranges of ints between 1 and N
  returnConsInts <- function(N) {
    stopifnot(N > 0)
    unlist(sapply(seq(1, N), function(ii) {
      return(sapply(seq(ii, N), function(jj) {
        return(seq(ii, jj))
      }))
    }), recursive = FALSE)
  }
  rangeNoHoles <- function(indicies, lr.tbl) {
    if (length(indicies) == 1) {
      return(TRUE)
    }
    sq <- seq(indicies[1], indicies[length(indicies)] - 1)
    all(lr.tbl[["l"]][sq + 1] <= lr.tbl[["r"]][sq])
  }
  per.drug.range.indicies <- sapply(conc.ranges.disj[, .N, drugid][, N], returnConsInts)

  names(per.drug.range.indicies) <- conc.ranges.disj[, unique(drugid)] ## checked this: conc.ranges.disj[,.N,drugid][,drugid] == conc.ranges.disj[,unique(drugid)]


  # Check if there are any holes in the chosen range combination
  per.drug.range.indicies <- sapply(names(per.drug.range.indicies), function(drug) {
    lr.tbl <- conc.ranges.disj[drugid == drug]
    per.drug.range.indicies[[drug]][sapply(per.drug.range.indicies[[drug]], rangeNoHoles, lr.tbl = lr.tbl)]
  })
  per.drug.range.indicies.2 <- sapply(names(per.drug.range.indicies), function(drug) {
    lr.tbl <- conc.ranges.disj[drugid == drug]
    res <- t(sapply(per.drug.range.indicies[[drug]], function(x) {
      return(c(lr.tbl[x[1], l], lr.tbl[x[length(x)], r]))
    }))
    colnames(res) <- c("l", "r")
    res <- data.frame(res)
    res <- cbind(drugid = drug, res)
  }, simplify = FALSE)
  per.drug.range.indicies.dt <- rbindlist(per.drug.range.indicies.2)

  conc.ranges <- conc.m[, .(l = min(value, na.rm = T), r = max(value, na.rm = T)), c("drugid", "Var1")]
  setkey(conc.m, Var1)
  conc.m <- na.omit(conc.m)
  setkey(conc.m, drugid, Var1, value)
  setkey(conc.ranges, drugid, l, r)
  # tic()
  ## NOTE:: Data.table used for maximum speed. Probably possible to do this more intelligently by
  ## NOTE:: being aware of which conditions overlap, but its fast enough right now as it is.
  chosen.drug.ranges <- lapply(unq.drugs, function(drug) {
    num.points.in.range <- apply(per.drug.range.indicies.dt[drugid == drug, .(l, r)], 1, function(rng) {
      conc.m[drugid == drug][conc.ranges[drugid == drug][l <= rng["l"]][r >= rng["r"], Var1], on = "Var1"][value >= rng["l"]][value <= rng["r"], .N]
      # conc.m[drugid==drug][, Var1]
    })
    max.ranges <- per.drug.range.indicies.dt[drugid == drug][which(num.points.in.range == max(num.points.in.range))]
    max.ranges[which.max(log10(r) - log10(l)), ]
  })
  # toc()
  names(chosen.drug.ranges) <- sapply(chosen.drug.ranges, `[[`, "drugid")
  removed.experiments <- unlist(lapply(unq.drugs, function(drug) {
    rng <- unlist(chosen.drug.ranges[[drug]][, .(l, r)])
    exp.out.range <- conc.ranges[drugid == drug][l > rng["l"] | r < rng["r"], Var1]
    return(exp.out.range)
  }))

  sens.raw[removed.experiments, , ] <- NA_real_
  conc.ranges.kept <- conc.ranges[!Var1 %in% removed.experiments]

  for (drug in unq.drugs) {
    rng <- unlist(chosen.drug.ranges[[drug]][, .(l, r)])
    myx <- conc.ranges.kept[drugid == drug, Var1]
    doses <- sens.raw[myx, , "Dose"]
    which.remove <- (doses < rng["l"] | doses > rng["r"])
    sens.raw[myx, , "Dose"][which(which.remove, arr.ind = TRUE)] <- NA_real_
    sens.raw[myx, , "Viability"][which(which.remove, arr.ind = TRUE)] <- NA_real_

    ## Annotate sens info with chosen range
    sens.info[sens.info$drugid == drug, "chosen.min.range"] <- rng["l"]
    sens.info[sens.info$drugid == drug, "chosen.max.range"] <- rng["r"]
  }
  sens.info$rm.by.conc.range <- FALSE
  sens.info[removed.experiments, "rm.by.conc.range"] <- TRUE

  return(list("sens.info" = sens.info, sens.raw = sens.raw))
}


# filter noisy curves from PSet (modified function to take into account standardized conc range)
filterNoisyCurves2 <- function(pSet, epsilon = 25, positive.cutoff.percent = .80, mean.viablity = 200, nthread = 1) {
  acceptable <- mclapply(rownames(sensitivityInfo(pSet)), function(xp) {
    # for(xp in rownames(sensitivityInfo(pSet))){
    drug.responses <- as.data.frame(apply(pSet@sensitivity$raw[xp, , ], 2, as.numeric), stringsAsFactors = FALSE)
    if (!all(is.na(drug.responses))) {
      drug.responses <- drug.responses[complete.cases(drug.responses), ]
      doses.no <- nrow(drug.responses)
      drug.responses[, "delta"] <- .computeDelta(drug.responses$Viability)

      delta.sum <- sum(drug.responses$delta, na.rm = TRUE)

      max.cum.sum <- .computeCumSumDelta(drug.responses$Viability)

      if ((table(drug.responses$delta < epsilon)["TRUE"] >= (doses.no * positive.cutoff.percent)) &
        (delta.sum < epsilon) &
        (max.cum.sum < (2 * epsilon)) &
        (mean(drug.responses$Viability) < mean.viablity)) {
        return(xp)
      }
    }
  }, mc.cores = nthread)
  acceptable <- unlist(acceptable)
  noisy <- setdiff(rownames(sensitivityInfo(pSet)), acceptable)
  return(list("noisy" = noisy, "ok" = acceptable))
}

.computeDelta <- function(xx, trunc = TRUE) {
  xx <- as.numeric(xx)
  if (trunc) {
    return(c(pmin(100, xx[2:length(xx)]) - pmin(100, xx[1:length(xx) - 1]), 0))
  } else {
    return(c(xx[2:length(xx)] - xx[1:length(xx) - 1]), 0)
  }
}

#' @importFrom utils combn
.computeCumSumDelta <- function(xx, trunc = TRUE) {
  xx <- as.numeric(xx)
  if (trunc) {
    xx <- pmin(xx, 100)
  }
  tt <- t(combn(1:length(xx), 2, simplify = TRUE))
  tt <- tt[which(((tt[, 2] - tt[, 1]) >= 2) == TRUE), ]
  if (is.null(nrow(tt))) {
    tt <- matrix(tt, ncol = 2)
  }
  cum.sum <- unlist(lapply(1:nrow(tt), function(x) {
    xx[tt[x, 2]] - xx[tt[x, 1]]
  }))
  return(max(cum.sum))
}

drug.info <- readRDS(file.path(processed_dir, "drug.info.rds"))
cell.info <- readRDS(file.path(processed_dir, "cell.info.rds"))

curationCell <- readRDS(file.path(processed_dir, "curationCell.rds"))
curationDrug <- readRDS(file.path(processed_dir, "curationDrug.rds"))
curationTissue <- readRDS(file.path(processed_dir, "curationTissue.rds"))

sens.info <- readRDS(file.path(processed_dir, "sens.info.rds"))
sens.prof <- readRDS(file.path(processed_dir, "sens.prof.rds"))

sens.raw <- readRDS(file.path(processed_dir, "sens.raw.rds"))

profiles <- get(load(file.path(processed_dir, "profiles.RData")))
profiles <- profiles[rownames(sens.info), ]
profiles <- apply(profiles, c(1, 2), as.numeric)

sens.prof <- cbind(sens.prof, profiles)

sens.prof <- sens.prof[, -c(1, 2)]

message("aac correlations are")

message(cor(sens.prof[, "aac_published"], sens.prof[, "aac_recomputed"], use = "pairwise.complete"))

emptyE <- ExpressionSet()
pData(emptyE)$cellid <- character()
pData(emptyE)$batchid <- character()
fData(emptyE)$BEST <- vector()
fData(emptyE)$Symbol <- character()
annotation(emptyE) <- "FIMM contains no molecular profiles of cell lines. This SE is empty placeholder."

emptySE <- SummarizedExperiment::SummarizedExperiment(
  ## TODO:: Do we want to pass an environment for better memory efficiency?
  assays = S4Vectors::SimpleList(as.list(Biobase::assayData(emptyE))),
  # Switch rearrange columns so that IDs are first, probes second
  rowData = S4Vectors::DataFrame(Biobase::fData(emptyE),
    rownames = rownames(Biobase::fData(emptyE))
  ),
  colData = S4Vectors::DataFrame(Biobase::pData(emptyE),
    rownames = rownames(Biobase::pData(emptyE))
  ),
  metadata = list(
    "experimentData" = emptyE@experimentData,
    "annotation" = Biobase::annotation(emptyE),
    "protocolData" = Biobase::protocolData(emptyE)
  )
)


cellsPresent <- sort(unique(sens.info$cellid))
cell.info <- cell.info[cellsPresent, ]


drugsPresent <- sort(unique(sens.info$drugid))

drug.info <- drug.info[drugsPresent, ]


drug_all <- read.csv(file.path(download_dir, "drugs_with_ids.csv"), na.strings = c("", " ", "NA"))
drug_all <- drug_all[which(!is.na(drug_all[, "FIMM.drugid"])), ]
drug_all <- drug_all[, c("unique.drugid", "FIMM.drugid", "smiles", "inchikey", "cid", "FDA")]
rownames(drug_all) <- drug_all[, "unique.drugid"]

drug_all <- drug_all[rownames(drug.info), ]
drug.info[, c("smiles", "inchikey", "cid", "FDA")] <- drug_all[, c("smiles", "inchikey", "cid", "FDA")]

curationCell <- curationCell[rownames(cell.info), ]
curationDrug <- curationDrug[rownames(drug.info), ]
curationTissue <- curationTissue[rownames(cell.info), ]

message("Making PSet")

sens.info <- as.data.frame(sens.info)

if (length(standardize) > 0) {
  # standardize <- standardizeRawDataConcRange(sens.info = sens.info, sens.raw = sens.raw)
  # sens.info<- standardize$sens.info
  # sens.raw <- standardize$sens.raw
} else {
  print("unfiltered PSet")
}

FIMM <- PharmacoGx::PharmacoSet(
  molecularProfiles = list("rna" = emptySE),
  name = "FIMM",
  cell = cell.info,
  drug = drug.info,
  sensitivityInfo = sens.info,
  sensitivityRaw = sens.raw,
  sensitivityProfiles = sens.prof,
  sensitivityN = NULL,
  curationCell = curationCell,
  curationDrug = curationDrug,
  curationTissue = curationTissue,
  datasetType = "sensitivity"
)


if (length(standardize) > 0) {
  noisy_out <- filterNoisyCurves2(FIMM)
  print("filter done")
  FIMM@sensitivity$profiles[noisy_out$noisy, ] <- NA
} else {
  print("unfiltered PSet")
}


message("Saving")

FIMM@annotation$version <- 2
saveRDS(FIMM, file = paste0(out_dir, filename), version = 2)


### CREATE BIOCOMPUTE OBJECT###


# ###########################
# ##### Provenance Domain#####
# ###########################

# # Created and modified dates
# # Sys.setenv(TZ = "EST")
# created <- as.POSIXct(Sys.time(), format = "%Y-%m-%dT%H:%M:%S", tz = "EST")
# modified <- as.POSIXct(Sys.time(), format = "%Y-%m-%dT%H:%M:%S", tz = "EST")

# # Contributions
# contributors <- data.frame(
#   "name" = c("Anthony Mammoliti", "Petr Smirnov", "Benjamin Haibe-Kains"),
#   "affiliation" = c(rep("University Health Network", 3)),
#   "email" = c("anthony.mammoliti@uhnresearch.ca", "petr.smirnov@utoronto.ca", "Benjamin.Haibe-Kains@uhnresearch.ca"),
#   "contribution" = c("createdBy", "createdBy", "authoredBy"),
#   "orcid" = c(NA, NA, "https://orcid.org/0000-0002-7684-0079"),
#   stringsAsFactors = FALSE
# )

# # License
# license <- "https://opensource.org/licenses/Apache-2.0"

# # Name of biocompute object
# name <- "FIMM"

# # Version of biocompute object
# bio_version <- "1.0.0"

# # Embargo (none)
# embargo <- c()

# # Derived from and obsolete after (none)
# derived_from <- c()
# obsolete_after <- c()

# # reviewers (none)
# review <- c()

# # compile domain
# provenance <- compose_provenance_v1.3.0(
#   name, bio_version, review, derived_from, obsolete_after,
#   embargo, created, modified, contributors, license
# )
# provenance %>% convert_json()


# ############################
# ##### Description Domain#####
# ############################
# times_rnaseq <- as.POSIXct("2020-01-20T1:04:10", format = "%Y-%m-%dT%H:%M:%S", tz = "EST")
# # Keywords and platform info
# keywords <- c("Biomedical", "Pharmacogenomics", "Cellline", "Drug")
# platform <- c("Pachyderm", "ORCESTRA (orcestra.ca)", "Linux/Ubuntu")

# # Metadata for each pipeline step
# pipeline_meta <- data.frame(
#   "step_number" = c("1", "2", "3"),
#   "name" = c(
#     "Curated Sample and treatment identifier compilation",
#     "Drug sensitivity processing",
#     "Build data object"
#   ),
#   "description" = c(
#     "Download of appropriate sample and treatment identifiers from GitHub (curations performed by BHK Lab - http://bhklab.ca)",
#     "Process sensitivity data",
#     "Building of ORCESTRA data object"
#   ),
#   "version" = c(1.0, 1.0, 1.0),
#   stringsAsFactors = FALSE
# )

# # Inputs for each pipeline step
# pipeline_input <- data.frame(
#   "step_number" = c("1", "1", "2", "3"),
#   "filename" = c(
#     "Sample annotation data",
#     "Treatment annotations",
#     "Raw sensitivity data",
#     "Script for data object generation"
#   ),
#   "uri" = c(
#     "https://github.com/BHKLAB-Pachyderm/Annotations/blob/master/cell_annotation_all.csv",
#     "https://github.com/BHKLAB-Pachyderm/Annotations/blob/master/drugs_with_ids.csv",
#     "https://media.nature.com/original/nature-assets/nature/journal/v540/n7631/extref/nature20171-s2.xlsx",
#     "https://github.com/BHKLAB-Pachyderm/getFIMM/getFIMM.R"
#   ),
#   "access_time" = c(created, created, created, created),
#   stringsAsFactors = FALSE
# )


# # Outputs for each pipeline step
# pipeline_output <- data.frame(
#   "step_number" = c("1", "1", "2", "2", "2", "3"),
#   "filename" = c(
#     "Downloaded sample annotations",
#     "Downloaded treatment annotations",
#     "Downloaded raw sensitivity data",
#     "Processed sensitivity data in parallel",
#     "Compiled sensitivity data",
#     "Data object"
#   ),
#   "uri" = c(
#     "/pfs/downAnnotations/cell_annotation_all.csv",
#     "/pfs/downAnnotations/drugs_with_ids.csv",
#     "/pfs/downFIMM/nature20171-s2.xlsx",
#     "/pfs/calculateFIMM/$_recomp.rds",
#     "/pfs/SliceAssemble/profiles.RData",
#     "/pfs/out/FIMM.rds"
#   ),
#   "access_time" = c(created, created, created, created, created, created),
#   stringsAsFactors = FALSE
# )

# # xref (none)
# xref <- c()

# # pipeline prereq (none)
# pipeline_prerequisite <- c()

# # compile domain
# description <- compose_description_v1.3.0(
#   keywords, xref, platform,
#   pipeline_meta, pipeline_prerequisite, pipeline_input, pipeline_output
# )
# description %>% convert_json()


# ############################
# ###### Execution Domain######
# ############################

# script <- c()
# script_driver <- c()

# # software/tools and its versions used for data object creation
# software_prerequisites <- data.frame(
#   "name" = c("Pachyderm", "Docker Image"),
#   "version" = c("1.9.3", "v3"),
#   "uri" = c(
#     "https://www.pachyderm.com", "https://hub.docker.com/r/bhklab/pharmacogx2.0"
#   ),
#   stringsAsFactors = FALSE
# )

# software_prerequisites[, "access_time"] <- rep(NA, length(software_prerequisites$name))
# software_prerequisites[, "sha1_chksum"] <- rep(NA, length(software_prerequisites$name))

# external_data_endpoints <- c()
# environment_variables <- c()

# execution <- compose_execution_v1.3.0(
#   script, script_driver, software_prerequisites, external_data_endpoints, environment_variables
# )
# execution %>% convert_json()


# ############################
# ###### Extension Domain######
# ############################

# # repo of scripts/data used
# scm_repository <- data.frame("extension_schema" = c("https://github.com/BHKLAB-Pachyderm"))
# scm_type <- "git"
# scm_commit <- c()
# scm_path <- c()
# scm_preview <- c()

# scm <- compose_scm(scm_repository, scm_type, scm_commit, scm_path, scm_preview)
# scm %>% convert_json()

# extension <- compose_extension_v1.3.0(scm)
# extension %>% convert_json()

# ############################
# ###### Parametric Domain#####
# ############################

# df_parametric <- data.frame(
#   "param" = c(),
#   "value" = c(),
#   "step" = c(),
#   stringsAsFactors = FALSE
# )

# parametric <- compose_parametric_v1.3.0(df_parametric)
# parametric %>% convert_json()



# ############################
# ###### Usability Domain######
# ############################

# # usability of our data objects
# text <- c(
#   "Pipeline for creating FIMM data object through ORCESTRA (orcestra.ca), a platform for the reproducible and transparent processing, sharing, and analysis of biomedical data."
# )

# usability <- compose_usability_v1.3.0(text)
# usability %>% convert_json()


# ######################
# ###### I/O Domain######
# ######################

# input_subdomain <- data.frame(
#   "step_number" = c("1", "1", "2", "3"),
#   "filename" = c(
#     "Sample annotation data",
#     "Treatment annotations",
#     "Raw sensitivity data",
#     "Script for data object generation"
#   ),
#   "uri" = c(
#     "https://github.com/BHKLAB-Pachyderm/Annotations/blob/master/cell_annotation_all.csv",
#     "https://github.com/BHKLAB-Pachyderm/Annotations/blob/master/drugs_with_ids.csv",
#     "https://media.nature.com/original/nature-assets/nature/journal/v540/n7631/extref/nature20171-s2.xlsx",
#     "https://github.com/BHKLAB-Pachyderm/getFIMM/getFIMM.R"
#   ),
#   "access_time" = c(created, created, created, created),
#   stringsAsFactors = FALSE
# )

# output_subdomain <- data.frame(
#   "mediatype" = c("csv", "csv", "xlsx", "RDS", "RData", "RDS"),
#   "uri" = c(
#     "/pfs/downAnnotations/cell_annotation_all.csv",
#     "/pfs/downAnnotations/drugs_with_ids.csv",
#     "/pfs/downFIMM/nature20171-s2.xlsx",
#     "/pfs/calculateFIMM/$_recomp.rds",
#     "/pfs/SliceAssemble/profiles.RData",
#     "/pfs/out/FIMM.rds"
#   ),
#   "access_time" = c(created, created, created, created, created, created),
#   stringsAsFactors = FALSE
# )

# io <- compose_io_v1.3.0(input_subdomain, output_subdomain)
# io %>% convert_json()


# ########################
# ###### Error Domain######
# ########################

# empirical <- c()
# algorithmic <- c()

# error <- compose_error(empirical, algorithmic)
# error %>% convert_json()


# #### Retrieve Top Level Fields####
# tlf <- compose_tlf_v1.3.0(
#   provenance, usability, extension, description,
#   execution, parametric, io, error
# )
# tlf %>% convert_json()


# #### Complete BCO####

# bco <- biocompute::compose_v1.3.0(
#   tlf, provenance, usability, extension, description,
#   execution, parametric, io, error
# )
# bco %>%
#   convert_json() %>%
#   export_json("/pfs/out/FIMM_BCO.json") %>%
#   validate_checksum()
