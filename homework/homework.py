"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import glob
import os
import zipfile
from io import BytesIO

import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    def load_data(input_directory):
        """Lee los archivos zip"""

        def unzip_files(input_directory):
            """Descomprime los archivos zip y los lee"""
            zip_files = glob.glob(os.path.join(input_directory, "*.zip"))
            for zip_file in zip_files:
                with zipfile.ZipFile(zip_file, "r") as zip_ref:
                    for file_info in zip_ref.infolist():
                        with zip_ref.open(file_info) as file:
                            yield BytesIO(file.read())

        def concat_readed_files(input_directory):
            """Genera un DataFrame con los archivos leidos"""
            return pd.concat(
                [
                    pd.read_csv(file, index_col=(0))
                    for file in unzip_files(input_directory)
                ],
                ignore_index=True,
            )

        return concat_readed_files(input_directory)

    def proccess_data(df, output_directory):

        def save_file(df, output_directory, file_name):
            """Guarda el archivo en la carpeta de salida"""
            folders = glob.glob(f"{output_directory}/*")
            if len(folders) >= 3:
                for file in folders:
                    os.remove(file)
                os.rmdir(output_directory)
            os.makedirs(output_directory, exist_ok=True)
            df.to_csv(os.path.join(output_directory, file_name), index=False)

        def df_client(df, output_directory):

            cols = [
                "client_id",
                "age",
                "job",
                "marital",
                "education",
                "credit_default",
                "mortgage",
            ]
            df = df.copy()
            df = df[cols]

            df["job"] = df["job"].str.replace(".", "").str.replace("-", "_")
            df["education"] = (
                df["education"].str.replace(".", "_").replace("unknown", pd.NA)
            )

            temp_function = lambda x: 1 if x == "yes" else 0
            df["credit_default"] = df["credit_default"].apply(temp_function)
            df["mortgage"] = df["mortgage"].apply(temp_function)

            save_file(df, output_directory, "client.csv")

        def df_campaign(df, output_directory):

            cols = [
                "client_id",
                "number_contacts",
                "contact_duration",
                "previous_campaign_contacts",
                "previous_outcome",
                "campaign_outcome",
                "last_contact_date",
            ]

            df = df.copy()
            df["month"] = pd.to_datetime(df["month"], format="%b").dt.month

            df["last_contact_date"] = pd.to_datetime(
                "2022-" + df["month"].astype(str) + "-" + df["day"].astype(str),
                format="%Y-%m-%d",
            )

            df["previous_outcome"] = df["previous_outcome"].apply(
                lambda x: 1 if x == "success" else 0
            )
            df["campaign_outcome"] = df["campaign_outcome"].apply(
                lambda x: 1 if x == "yes" else 0
            )

            df = df[cols]

            save_file(df, output_directory, "campaign.csv")

        def df_economics(df, output_directory):

            cols = ["client_id", "cons_price_idx", "euribor_three_months"]
            df = df.copy()
            df = df[cols]

            save_file(df, output_directory, "economics.csv")

        df_client(df, output_directory)
        df_campaign(df, output_directory)
        df_economics(df, output_directory)

    def run_job(input_directory, output_directory):
        df = load_data(input_directory)
        proccess_data(df, output_directory)

    run_job("files/input", "files/output")

    return "Archivos generados satisfactoriamente"


if __name__ == "__main__":
    print(clean_campaign_data())
