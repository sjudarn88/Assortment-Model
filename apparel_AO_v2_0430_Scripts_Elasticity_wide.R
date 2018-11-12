args <- commandArgs()
library(data.table)
store <- args[6]
div_str <- args[7]
div <- as.numeric(strsplit(args[7], "_")[[1]])
soar <- args[8]
season <- args[9]

#create data frame
dt <- fread(paste0("./", store, "_", soar, "_", div_str,"_",season, "/elasticity_data_store_", store, "_", soar,"_", div_str,"_",season, ".csv"))
#keep certain columns
dt <- dt[div_no %in% div, .(div_ln_cls, wk_nbr, log_UNITS, log_price)]
temp <- copy(dt)
temp_w <- dcast(temp, formula = wk_nbr ~ div_ln_cls, value.var = "log_price")
temp_w <- merge(temp[,.(wk_nbr, div_ln_cls, log_UNITS)], temp_w, by ="wk_nbr")
write.csv(temp_w, paste0("./", store, "_", soar, "_", div_str, "_",season,"/elasticity_data_store_", store, "_", soar, "_", div_str, "_wide.csv"))

print("Completed")
