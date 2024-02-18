#!/bin/bash

DATA_DIR="/Users/guanlil1/Dropbox/PostDoc/topics/MAB/DSGen-software-code-3.2.0rc1/tools/data"

# for file in inventory.dat call_center.dat catalog_page.dat catalog_returns.dat catalog_sales.dat customer_address.dat customer_demographics.dat customer.dat date_dim.dat dbgen_version.dat household_demographics.dat income_band.dat item.dat promotion.dat reason.dat ship_mode.dat store_returns.dat store_sales.dat store.dat time_dim.dat warehouse.dat web_page.dat web_returns.dat web_sales.dat web_site.dat
for file in customer.dat
do
  echo "Processing $file..."
  LC_ALL=C sed -i '' 's/|$//' "$DATA_DIR/$file"
done

echo "All files processed."
