setwd("/Users/urischwartz/PycharmProjects/mentalSearches")

install.packages("broom")
install.packages("dplyr")
install.packages("tidyr")
install.packages("openxlsx")  # For writing to Excel

library(broom)
library(dplyr)
library(tidyr)
library(openxlsx)  # Load openxlsx for writing to Excel

df <- read.csv("mental_search_trend_dataframe.csv")

# Convert 'month' to a factor
df$month <- factor(df$month)

# Independent and dependent variables
independent_vars <- c("X", "month", "is_weekend", "is_jewish_holiday", "is_national_memorial_day", "is_quarantine", "is_after_oct_7")
dependent_vars <- c("average_total_search_rate", "average_info_search_rate", "average_help_search_rate", "Major.depressive.disorder...Israel.", "Anxiety...Israel.", "Loneliness...Israel.", "Sadness...Israel.", "Panic.attack...Israel.", "Suicide...Israel.", "Post.traumatic.stress.disorder...Israel.", "Psychological.trauma...Israel.", "Bipolar.disorder...Israel.", "Psychologist...Israel.", "Psychiatrist...Israel.", "Clonazepam...Israel.", "Escitalopram...Israel.", "Antidepressant...Israel.")

# Run regressions and collect results
results_list <- lapply(dependent_vars, function(dep_var) {
  formula <- as.formula(paste(dep_var, "~", paste(independent_vars, collapse = " + ")))  # Create formula for each dependent variable
  model <- lm(formula, data = df)  # Fit linear model
  
  tidy(model) %>%
    filter(term != "(Intercept)") %>%  # Exclude intercept term from results
    mutate(
      dependent_variable = dep_var,
      term = gsub("True", "", term),  # Remove "True" from boolean variable names
      estimate = round(estimate, 2)  # Round the estimate to 2 decimal places
    ) %>%
    select(dependent_variable, term, estimate, p.value) %>%
    mutate(significance = case_when(  # Add column for significance based on p-value
      p.value < 0.001 ~ "***",
      p.value < 0.01 ~ "**",
      p.value < 0.05 ~ "*",
      TRUE ~ ""
    )) %>%
    unite("estimate_significance", estimate, significance, sep = " ") %>%  # Combine estimate and significance into one column
    mutate(r_squared = summary(model)$r.squared)  # Add R-squared value for the model
})

# Combine results into a single data frame
results_df <- bind_rows(results_list) %>%
  select(dependent_variable, term, estimate_significance, r_squared)

# Reshape the data frame
results_wide <- results_df %>%
  pivot_wider(names_from = term, values_from = estimate_significance)  # Reshape data frame to wide format

# Display the results
print(results_wide)

# Create Excel workbook and add data with color coding
wb <- createWorkbook()
sheet_name <- "Regression Results"
addWorksheet(wb, sheet_name)
writeData(wb, sheet_name, results_wide, startCol = 1, startRow = 1)

# Save Excel file
saveWorkbook(wb, "Regression_Results.xlsx", overwrite = TRUE)
