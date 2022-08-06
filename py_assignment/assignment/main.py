import pandas as pd
import numpy as np
from datetime import datetime


def quality_check(csv_file, to_check, postal_code_default, state_territory_default):
    df = pd.DataFrame(csv_file)
    log_file = open("quality_check_log_file.txt", "w+")

    for i in range(len(to_check)):

        log_file.write("----------------------- " + to_check[i] + " quality check -----------------------" + "\n")

        if df[to_check[i]].isnull().values.any():
            if to_check[i] == 'POSTAL_CODE':
                log_file.write(
                    "The following ORDER_NO : \n" +
                    df.loc[df[df[to_check[i]].isnull()].index.to_list(), 'ORDER_NO'].to_string(
                        index=False) + " : in csv file are Null/NaN/NA : default value added : "
                    + postal_code_default + " : " + str(df[to_check[i]].isnull().sum()) + " null values \n")
                df[to_check[i]].fillna(str(postal_code_default), inplace=True)
            else:
                log_file.write(
                    "The following ORDER_NO : \n" +
                    df.loc[df[df[to_check[i]].isnull()].index.to_list(), 'ORDER_NO'].to_string(
                        index=False) + " : in csv file are Null/NaN/NA : default value added : "
                    + state_territory_default + " : " + str(df[to_check[i]].isnull().sum()) + " null values \n")
                df[to_check[i]].fillna(state_territory_default, inplace=True)

    log_file.close()

    df.to_csv("applied_quality_checks_sales.csv", index=False)


def consistency_checks(csv_file):
    df = pd.DataFrame(csv_file)
    log_file = open("consistency_check_log_file.txt", "w+")

    errors_num = 0
    log_file.write(
        "----------------------- TERRITORIES CORRECTNESS (only checking if all letters are CAPS) -----------------------" + "\n")
    for index in range(len(df)):
        if not str(df.loc[index, 'TERRITORY']).isupper() and df.loc[index, 'TERRITORY'] is not np.nan:
            errors_num += 1
            log_file.write("ORDER_NO : " + str(df.loc[index, 'ORDER_NO']) + " : value = " + str(
                df.loc[index, 'TERRITORY']) + " : is not all CAPS" + "\n")

    if errors_num == 0:
        log_file.write("--* All good *--" + "\n")

    log_file.write(
        "----------------------- " + str(errors_num) + " values with NOT ALL CAPS -----------------------" + "\n")

    log_file.write(
        "----------------------- DATE CORRECTNESS -----------------------" + "\n")
    errors_num = 0
    for index in range(len(df)):
        order_date = datetime.strptime(df.loc[index, 'ORDER_DATE'], '%m/%d/%Y %H:%M')
        if not (order_date.month == int(df.loc[index, 'MONTH_ID'])):
            errors_num += 1
            log_file.write(
                "ORDER_NO : " + str(
                    df.loc[index, 'ORDER_NO']) + " : MONTH_ID does not correspond with ORDER_DATE" + "\n")
        if not (order_date.year == int(df.loc[index, 'YEAR_ID'])):
            errors_num += 1
            log_file.write(
                "ORDER_NO : " + str(
                    df.loc[index, 'ORDER_NO']) + " : YEAR_ID does not correspond with ORDER_DATE" + "\n")
        if not (((1 <= order_date.month <= 3) and int(df.loc[index, 'QUATER_ID']) == 1) or (
                (4 <= order_date.month <= 6) and int(df.loc[index, 'QUATER_ID']) == 2) or (
                        (7 <= order_date.month <= 9) and int(df.loc[index, 'QUATER_ID']) == 3) or (
                        (10 <= order_date.month <= 12) and int(df.loc[index, 'QUATER_ID']) == 4)):
            errors_num += 1
            log_file.write(
                "ORDER_NO : " + str(df.loc[index, 'ORDER_NO']) + " : wrong QUATER_ID" + "\n")
    if errors_num == 0:
        log_file.write("--* All good *--" + "\n")

    log_file.write("----------------------- " + str(errors_num) + " date errors -----------------------" + "\n")

    log_file.close()


def save_csv(df, file_name):
    df.to_csv(file_name)


def discount_surcharge(df, grouped_by):
    new_df = pd.DataFrame()
    df['DIS_SUR'] = (df['UNIT_PRICE'] - df['MSRP']) * df['ORDER_QTY']
    new_df[['TOTAL_DISCOUNT/SURCHARGE']] = df.groupby(grouped_by)[['DIS_SUR']].sum()
    df = df.drop('DIS_SUR', axis=1)

    return new_df[['TOTAL_DISCOUNT/SURCHARGE']]


def total_by(csv_file, grouped_by, file_name):
    df = pd.DataFrame(csv_file)
    totals_df = pd.DataFrame()

    totals_df[['TOTAL_SALES', 'TOTAL_QUANTITY']] = df.groupby(grouped_by)[['SALES', 'ORDER_QTY']].sum()
    totals_df[['TOTAL_DISCOUNT/SURCHARGE']] = discount_surcharge(df, grouped_by)

    save_csv(totals_df, file_name)


def most_successful(df, grouped_by, file_name):
    new_df = pd.DataFrame()
    new_df['TOTAL_SALES'] = df.groupby(grouped_by)["SALES"].sum()
    new_df = new_df.loc[new_df.groupby(["COUNTRY", "YEAR_ID"])['TOTAL_SALES'].idxmax()]

    save_csv(new_df, file_name)


def monthly_total_sales(df, file_name):
    save_csv(df.groupby(["COUNTRY", "TERRITORY", "MONTH_ID"])[["SALES"]].sum(), file_name)


def cumulative_total(df, file_name):
    save_csv(df.groupby(["MONTH_ID"])["SALES"].cumsum(), file_name)


def highest_selling_product_line(df, grouped_by, file_name):
    new_df = pd.DataFrame()
    new_df[['TOTAL_SALES', 'TOTAL_QUANTITY']] = df.groupby(grouped_by)[['SALES', 'ORDER_QTY']].sum()
    new_df[['TOTAL_DISCOUNT/SURCHARGE']] = discount_surcharge(df, grouped_by)

    save_csv(new_df.loc[new_df.groupby(["COUNTRY"])['TOTAL_SALES'].idxmax()], file_name)


if __name__ == '__main__':
    filename = 'sales.csv'
    csv_sales_file = pd.read_csv(filename)
    # print(csv_sales_file.info())

    quality_check(csv_sales_file, ['POSTAL_CODE', 'STATE', 'TERRITORY'], '000000', 'Not Available')
    consistency_checks(csv_sales_file)

    # c=pd.read_csv('applied_quality_checks_sales.csv')
    # print(c.info())

    data_frame = pd.DataFrame(csv_sales_file)

    # Total Sales, Total Quantity, Total Discount/Surcharge, COUNTRY, STATE, STATUS
    total_by(csv_sales_file, ['COUNTRY', 'STATE', 'STATUS'], 'total_sales_qty_dis_sur_by_css.csv')

    # Highest Selling Product Line in a Country, Total Quantity Sold, Total Sales, Total Discount/Surcharge of this
    # Product Line
    highest_selling_product_line(data_frame, ["COUNTRY", "PRODUCT_LINE"], "highest_selling_product_line.csv")

    # Most Successful Month for each year and country, Total Sales
    most_successful(data_frame, ["COUNTRY", "YEAR_ID", "MONTH_ID"], "most_successful_month.csv")

    # Most Successful Quarter for each year and country, Total Sales
    most_successful(data_frame, ["COUNTRY", "YEAR_ID", "QUATER_ID"], "most_successful_quater.csv")

    # Total Sales, Total Quantity, Total Discount/Surcharge, DEAL_SIZE
    total_by(csv_sales_file, ['DEAL_SIZE'], 'total_sales_qty_dis_sur_by_deal_size.csv')

    # Monthly Total Sales for each Country and Territory
    monthly_total_sales(data_frame, "monthly_total_sales.csv")

    # Cumulative Total Sales per Month
    cumulative_total(data_frame, "cumulative_total.csv")
