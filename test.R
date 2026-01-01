# read a .csv file from the dropbox to see if I need to make the file available
# offline before a script interacts with it to use its contents

path <- "/mnt/c/Users/zeppo/Dropbox/SLES/State Legislative Data/States/WY/WY_Bill_Details_2023.csv"
df <- read.csv(path)
head(df)