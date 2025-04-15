# scripts/data_cleaning.py

from sqlalchemy import create_engine
import pandas as pd
import os

RAW_DATA_DIR = "../data/raw"
PROCESSED_DATA_DIR = "../data/processed"


def clean_and_merge_data():
    print("Loading and cleaning data...")

    # Load files
    sale_report = pd.read_csv(os.path.join(RAW_DATA_DIR, "Sale Report.csv"))
    pl_march = pd.read_csv(os.path.join(RAW_DATA_DIR, "P  L March 2021.csv"))
    may_2022_v = pd.read_csv(os.path.join(RAW_DATA_DIR, "May-2022.csv"))
    cloud_warehouse_compersion_chart_df = pd.read_csv(os.path.join(
        RAW_DATA_DIR, "Cloud Warehouse Compersion Chart.csv"))
    amazon_sale_report_df = pd.read_csv(os.path.join(
        RAW_DATA_DIR, "Amazon Sale Report.csv"))
    intl_sales = pd.read_csv(os.path.join(
        RAW_DATA_DIR, "International sale Report.csv"))

    # Cleaning
    amazon_sales_report(amazon_sale_report_df)
    cloud_warehouse_report(cloud_warehouse_compersion_chart_df)
    international_sales_report(intl_sales)
    may_2022(may_2022_v)
    p_l_march_2021(pl_march)
    sales_report(sale_report)

    print("Data cleaning and merging complete.")


def df_to_db(df: pd.DataFrame, table_name: str):
    """Load DataFrame to MySQL database"""
    # Create engine
    engine = create_engine(
        "mysql+pymysql://root@localhost:3306/e-commerce_sales")

    # Load DataFrame to MySQL table
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

    # Dispose of the engine
    engine.dispose()

def amazon_sales_report(df: pd.DataFrame):
    """Process Amazon sales report DataFrame"""

    # fix "Courier Status" column nan -> Unknown
    df["Courier Status"] = df["Courier Status"].fillna(
        "Unknown")

    # fix currency column nan -> INR
    df["currency"] = df["currency"].fillna(
        "INR")
    # fix Amount column nan -> 0.0
    df["Amount"] = df["Amount"].fillna(
        0.0)

    # fix Amount column to float
    # every value should follow this format -> .2f
    df["Amount"] = df["Amount"].astype(
        float)

    # remove leading and trailing whitespace
    df["Date"] = df["Date"].str.strip()
    # convert to datetime format
    df["Date"] = pd.to_datetime(
        df["Date"], format="%m-%d-%y")

    # drop the following columns: promotion-ids, fulfilled-by, Unnamed: 22, ship-service-level, Sales Channel, ship-city, ship-state, ship-postal-code, ship-country, Status
    df.drop(columns=["promotion-ids", "fulfilled-by", "Unnamed: 22", "ship-service-level",
                     "Sales Channel ", "ship-city", "ship-state", "ship-postal-code", "ship-country", "Status"], inplace=True)
    # check the percentage of missing values in each column again
    df.isnull().mean() * 100

    # drop index column and rename currency column to Currency
    df.drop(columns=["index"], inplace=True)
    df.rename(
        columns={"currency": "Currency"}, inplace=True)

    # save the cleaned data to csv
    df.to_csv(os.path.join(PROCESSED_DATA_DIR,
              "amazon_sales_report_cleaned.csv"), index=False)
    print("Amazon sales report cleaned and saved to processed/n")

    # load the cleaned data to mysql database
    df_to_db(df, "amazon_sales_report")

def cloud_warehouse_report(df: pd.DataFrame):
    """Process Cloud Warehouse report DataFrame"""

    # drop index column
    df.drop(columns=["index"], inplace=True)
    df = df.drop(
        0).reset_index(drop=True)

    # and also drop everything from row 4 onwards
    df.drop(
        df.index[4:], inplace=True)

    # rename columns
    df.columns = [
        "Cost Head", "Shiprocket", "INCREFF"]

    # Clean Shiprocket column: remove ₹ and commas, handle non-numeric gracefully
    df["Shiprocket"] = df["Shiprocket"].str.replace(
        r"[₹,]", '', regex=True).str.strip()

    # Clean INCREFF column: remove Rs, /-, Per Day, commas etc.
    df["INCREFF"] = df["INCREFF"].str.replace(
        r"Rs|/-|Per Day|,", '', regex=True).str.strip()

    # Attempt to convert to numeric (invalid parsing will result in NaN)
    df["Shiprocket"] = pd.to_numeric(
        df["Shiprocket"], errors="coerce")

    df["INCREFF"] = pd.to_numeric(
        df["INCREFF"], errors="coerce")

    # Drop rows where both Shiprocket & INCREFF are NaN
    cloud_warehouse_compersion_chart_df_cleaned = df.dropna(
        subset=["Shiprocket", "INCREFF"], how="all").reset_index(drop=True)

    # Clean Cost Head text: remove extra spaces, line breaks etc.
    cloud_warehouse_compersion_chart_df_cleaned["Cost Head"] = cloud_warehouse_compersion_chart_df_cleaned["Cost Head"].astype(
        str).str.replace(r"\s+", ' ', regex=True).str.strip()

    df_to_db(cloud_warehouse_compersion_chart_df_cleaned,
             "cloud_warehouse_report")

    # Save cleaned data to CSV
    cloud_warehouse_compersion_chart_df_cleaned.to_csv(os.path.join(
        PROCESSED_DATA_DIR, "cloud_warehouse_report_cleaned.csv"), index=False)
    print("Cloud Warehouse report cleaned and saved to processed/")

def international_sales_report(df: pd.DataFrame):

    # rename columns
    df = df.rename(columns={
        "DATE": "Date",
        "CUSTOMER": "Customer_name",
        "PCS": "Pieces_sold",
        "RATE": "Price_per_piece",
        "GROSS AMT": "Gross_amount",
    })

    # drop nans
    df.dropna(inplace=True)

    # fix Date Months and Customer_name columns
    # in some rows, the Customer_name is in the Date column and the Date is in the Customer_name column
    # and in some rows the Month in is Customer_name column and the Date is in the Month column
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    # check if the first 3 characters of the Customer_name column are in the months list
    # if yes, switch the columns
    for index, row in df.iterrows():
        if row["Customer_name"][:3] in months:
            # switch the columns
            temp = row["Customer_name"]
            df.at[index, "Customer_name"] = row["Date"]
            df.at[index, "Date"] = temp

    # check if the first 3 characters of the Date column are in the months list
    # if yes, switch the columns
    for index, row in df.iterrows():
        if row["Date"][:3] in months:
            # switch the columns
            temp = row["Date"]
            df.at[index, "Date"] = row["Months"]
            df.at[index, "Months"] = temp

    # there should be more than 1 dashes in the SKU column
    # check if the SKU column has more than 1 dashes
    # if not remove rows with invalid SKU
    df = df[df["SKU"].str.count(
        "-") > 1]

    # the Size column should have only s,m,l,xl,xxl,xxxl
    # if not remove rows with invalid Size
    df = df[df["Size"].isin(
        ["S", "M", "L", "XL", "XXL", "XXXL"])]

    # rest index column
    df.reset_index(drop=True, inplace=True)
    # drop the index column
    df.drop(columns=["index"], inplace=True)

    # convert the Date column to datetime
    df["Date"].str.strip()
    df["Date"] = pd.to_datetime(
        df["Date"], format="%m-%d-%y")
    # convert Months column to datetime
    df["Months"].str.strip()
    df["Months"] = pd.to_datetime(
        df["Months"], format="%b-%y").dt.strftime("%b-%y")

    # convert the Pieces_sold column to int
    df["Pieces_sold"] = df["Pieces_sold"].str.strip(".00").astype(int)
    # convert the Price_per_piece column to float
    df["Price_per_piece"] = df["Price_per_piece"].str.strip(
    ).astype(float)
    # convert the Gross_amount column to float
    df["Gross_amount"] = df["Gross_amount"].str.strip(
    ).astype(float)

    df_to_db(df, "international_sales_report")
    # Save cleaned data to CSV
    df.to_csv(os.path.join(
        PROCESSED_DATA_DIR, "international_sales_report_cleaned.csv"), index=False)
    print("International sales report cleaned and saved to processed/n")

def may_2022(df: pd.DataFrame):
    """Process May 2022 report DataFrame"""

    # drop duplicates
    df.drop_duplicates(inplace=True)

    # remove the noise. i.e unnecessary columns like:
    # Style Id

    df.drop(columns=["Style Id"], inplace=True)

    # save the cleaned data to csv
    df.to_csv(os.path.join(PROCESSED_DATA_DIR,
              "may_2022_report_cleaned.csv"), index=False)
    print("May 2022 report cleaned and saved to processed/n")

    df_to_db(df, "may_2022_report")

def p_l_march_2021(df: pd.DataFrame):
    """Process P&L March 2021 report DataFrame"""

    # drop Style Id column
    df.drop(columns=["Style Id"], inplace=True)

    # Define a helper function to clean and convert columns
    def clean_and_convert(column_name: str, replace_value=["Nill", "#VALUE!"], default_value=0.0, to_type=float):
        df[column_name] = df[column_name].replace(
            replace_value, str(default_value)).astype(to_type)
        if to_type == float:
            df[column_name] = df[column_name].astype(int)

    # List of columns to clean and convert
    columns_to_clean = [
        "Weight", "TP 1", "TP 2", "MRP Old", "Final MRP Old", "Ajio MRP",
        "Amazon MRP", "Amazon FBA MRP", "Flipkart MRP", "Limeroad MRP",
        "Myntra MRP", "Paytm MRP", "Snapdeal MRP"
    ]

    # Apply the helper function to each column
    for column in columns_to_clean:
        clean_and_convert(column_name=column)

    # save the cleaned data to csv
    df.to_csv(os.path.join(PROCESSED_DATA_DIR,
              "p_l_march_2021_report_cleaned.csv"), index=False)
    print("P&L March 2021 report cleaned and saved to processed/n")

    # load the cleaned data to mysql database
    df_to_db(df, "p_l_march_2021_report")

def sales_report(df: pd.DataFrame):

    # drop rows that have missing values in the following columns: SKU Code, Design No., Stock, Category, Size, Color
    df.dropna(subset=["SKU Code", "Design No.",
             "Stock", "Category", "Size", "Color"], inplace=True)

    # cast columns to appropriate data types
    df["SKU Code"] = df["SKU Code"].astype(str)
    df["Design No."] = df["Design No."].astype(str)
    df["Stock"] = df["Stock"].astype(int)
    df["Category"] = df["Category"].astype(str)
    df["Size"] = df["Size"].astype(str)
    df["Color"] = df["Color"].astype(str)

    # reset the index of the DataFrame
    df.reset_index(drop=True, inplace=True)

    # save the cleaned data to csv
    df.to_csv(os.path.join(PROCESSED_DATA_DIR,
              "sale_report_cleaned.csv"), index=False)
    print("Sale report cleaned and saved to processed/n")

    # load the cleaned data to mysql database
    df_to_db(df, "sale_report")


if __name__ == "__main__":
    clean_and_merge_data()
