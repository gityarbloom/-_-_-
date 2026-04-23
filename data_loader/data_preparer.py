import pandas as pd
import json



class DataPreparer:

    def __init__(self, gps_path:str, intercepts_path:str, csv_path:str):
        self.gps_path = gps_path
        self.intercepts_path = intercepts_path
        self.df = self.loading_csv(csv_path)

    @staticmethod
    def loading_json(file_path:str, opening_way="r"):
        with open(file_path, opening_way, encoding='utf-8') as f:
            return json.load(f)
        

    def loading_csv(self, csv_path):
        return pd.read_csv(csv_path, na_values=[""], keep_default_na=True).fillna("UNKNOWN")
        

    def create_suspects_df(self):
        suspects_df = self.df[["suspect_id", "full_name", "nationality", "last_known_location"]]
        return suspects_df


    def create_accounts_financial_df(self):
        accounts_financial = self.df[["suspect_id", "bank_account", "initial_risk"]].copy()
        accounts_financial["credit_rating_factor"] = pd.cut(
            self.df["initial_risk"],
            bins=[0, 3, 6, 8, 10],
            labels=[1, 3, 4, 5]
        )
        return accounts_financial