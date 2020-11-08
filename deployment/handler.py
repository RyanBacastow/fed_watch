#!/usr/bin/env python
# coding: utf-8
from os import environ as env
import os
import boto3
import pandas as pd
import pandas_datareader as pdr
from datetime import datetime
from contextlib import closing
import json
import traceback

sep = '\n--------------------------------------------------------------------------------------------\n'

def publish_message_sns(message):
    """
    :param message: str: message to be sent to SNS
    :return: None
    """
    sns_arn = env.get('SNS_ARN').strip()
    sns_client = boto3.client('sns')
    try:
        response = sns_client.publish(
            TopicArn=sns_arn,
            Message=message
        )

    except Exception as e:
        print(f"ERROR PUBLISHING MESSAGE TO SNS: {e}")


def get_data():
    """
    :param tickers: str: stock ticker string
    :param period: str: valid date period for comparison
    :return: temp_string, delta: str, float: stock printing statements and ratio are returned
    """
    start = env['START_DATE']
    cols = ['Fed BS', 'ECB BS', 'EURUSD', 'SPX']
    df = pdr.DataReader(['WALCL', 'ECBASSETSW', 'DEXUSEU', 'SP500'], 'fred', start)
    df.columns = cols

    df = df.resample('W').last()
    df = df.fillna(method='ffill')
    df = df.dropna()

    df['ECB in USD'] = df['ECB BS'] * df['EURUSD']
    df['Tot BS'] = df['ECB in USD'] + df['Fed BS']
    df['BS %Chg'] = df['Tot BS'].pct_change(12)
    df['SPX % chg'] = df['SPX'].pct_change(12)

    return df

def handler(event, context):
    """
    This function drives the AWS lambda. Requires 1 env var to work correctly: SNS_TOPIC which represents the topic arn to which
    you want to publish.
    """
    global sep
    message = f"""{sep}FED WATCH CENTRAL BANK ANALYSIS{sep}"""

    df = get_data()
    print(df)

    publish_message_sns(message)
    return message
