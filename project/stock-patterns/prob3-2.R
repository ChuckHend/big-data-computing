require(ggplot2)

setwd("T:/OneDrive/Academics/Data Science/DS730_Big_Data/big-data-computing/final_project/partC/")

# infile is the output of prob3-2.scala
infile = 'sector-peaks.csv'
d <- read.csv('sector-peaks.csv')

ggplot(d, aes(x=hr,y=count_scaled)) +
  geom_bar(stat='identity') +
  facet_wrap(~sector) +
  scale_fill_brewer(palette = 'Set1') +
  xlab("Time of Day\n (24-hour)") +
  ylab("Proportion of Peak Prices")
