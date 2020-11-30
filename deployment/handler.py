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
import matplotlib.pyplot as plt
import textwrap
import matplotlib.dates as dates
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from datetime import datetime
import traceback
from botocore.exceptions import ClientError

sep = '\n--------------------------------------------------------------------------------------------\n'

MA1 = int(env['MA1'])
MA2 = int(env['MA2'])
DD_to_buy = -(float(env['DD']))
# REMOVE_LAST52 = False

def create_filename(title, ext):
    return f"{title}_{datetime.utcnow().strftime('%Y_%m_%d')}.{ext}"


def s3_upload(filename, dir):
    """
    :param filename: str: outfile name
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(env['BUCKET_NAME'])

    try:
        bucket.upload_file(f"/tmp/{filename}", f"{dir}/{filename}", ExtraArgs={'ACL': 'public-read'})
        url = f"https://s3.amazonaws.com/{env['BUCKET_NAME']}/{dir}/{filename}"
        print(f"Successfully uploaded to {url}.")
        return url
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        return None


def create_img(x, x2, title, FileName, IncMostRecent = False, TwoOnFirstPanel = False, TwoAxis = True, Color2 = '#c4bd97', Panel1x2 = '', title2 = '', label1 = '', label2 = '', label2forpanel1 = '', HandleDate = True, CMblue = '#17375E'):

    plt.rcParams['ytick.right'] = plt.rcParams['ytick.labelright'] = True
    plt.rcParams['ytick.left'] = plt.rcParams['ytick.labelleft'] = False
    fig, (ax, ax2) = plt.subplots(2, 1, figsize=(8, 8), sharex=True)
    if TwoAxis == True:
        fig.subplots_adjust(right=0.9)

    ax.set_title(title, fontsize=18, fontweight='bold', color='#17375E')

    def make_patch_spines_invisible(ax):
        ax.set_frame_on(True)
        ax.patch.set_visible(False)
        for sp in ax.spines.values():
            sp.set_visible(False)

    if TwoOnFirstPanel == True:
        if TwoAxis == True:
            par2 = ax.twinx()
            par2.spines["right"].set_position(("axes", 1.05))
            par2.plot(Panel1x2, color=Color2, label=label2forpanel1, alpha=0.85)
            par2.yaxis.label.set_color(Color2)
            par2.legend(loc='center left')
            par2.tick_params(axis='y', colors=Color2)
            par2.spines['left'].set_visible(False)
            par2.spines['top'].set_visible(False)
            par2.spines['bottom'].set_color(CMblue)
            par2.spines['right'].set_color(Color2)
        else:
            ax.plot(Panel1x2, color=Color2, label=label2forpanel1, alpha=0.85)

    ax.plot(x, label=label1, color=CMblue)
    if IncMostRecent:
        color = 'red' if x[-1] < x[-2] else 'green'
        ax.plot(x[-2:], color=color, label='most recent')
    ax.legend(fontsize=14)
    # plt.yticks(size = 14)

    ax.yaxis.set_ticks_position('right')
    ax.legend(loc='upper left')
    ax.spines['left'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.spines['bottom'].set_color(CMblue)
    ax.spines['right'].set_color(CMblue)
    ax.xaxis.label.set_color(CMblue)
    ax.tick_params(axis='x', colors=CMblue)
    ax.tick_params(axis='y', colors=CMblue)
    ####### THIS WILL SET MAX TICK LABELS FOR X AXIS #########
    ax.xaxis.set_major_locator(plt.MaxNLocator(10))

    # ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    ax2.plot(x2, label=label2)
    if IncMostRecent:
        color = 'green' if x2[-1] < x2[-2] else 'red'
        ax2.plot(x2[-2:], color=color, label='most recent')

    ax2.set_title(title2, fontsize=11, fontweight='bold', style='italic', color=CMblue)
    ax2.yaxis.set_ticks_position('right')
    ax2.legend(loc='upper left')
    ax2.spines['left'].set_visible(False)
    ax2.spines['top'].set_visible(False)
    ax2.spines['bottom'].set_color(CMblue)
    ax2.spines['right'].set_color(CMblue)
    ax2.xaxis.label.set_color(CMblue)
    ax2.tick_params(axis='x', colors=CMblue)
    ax2.tick_params(axis='y', colors=CMblue)
    ax2.xaxis.set_major_locator(plt.MaxNLocator(10))

    fig.autofmt_xdate()
    ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    ax2.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')

    if HandleDate:
        myFmt = mdates.DateFormatter('%b-%Y')
        ax.xaxis.set_major_formatter(myFmt)
    # ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    # plt.locator_params(axis='x', nbins=10)
    plt.tight_layout()
    plt.savefig(f"/tmp/{FileName}")


def get_data():
    start = env['START_DATE']
    cols = ['Fed BS', 'ECB BS', 'EURUSD', 'SPX']
    df = pdr.DataReader(['WALCL', 'ECBASSETSW', 'DEXUSEU', 'SP500'], 'fred', start)
    df.columns = cols

    df = df.resample('W').last()
    df = df.fillna(method='ffill')
    df = df.dropna()
    df['ECB in USD'] = df['ECB BS'] * df['EURUSD']
    df['Tot BS'] = df['ECB in USD'] + df['Fed BS']

    df['BS 12wk %Chg'] = df['Tot BS'].pct_change(12)
    df['SPX 12wk % chg'] = df['SPX'].pct_change(12)
    df['BS 1wk %Chg'] = df['Tot BS'].pct_change(1)
    df['SPX 1wk % chg'] = df['SPX'].pct_change(1)

    return df


def makeMASignals(df, PriceCol, MA1 = MA1, MA2 = MA2, NoPriceCol = True, LongOnly = True):
    df = df[[PriceCol]]
    df[PriceCol + '_MA'+str(MA1)] = df[PriceCol].rolling(MA1).mean()
    df[PriceCol + '_MA'+str(MA2)] = df[PriceCol].rolling(MA2).mean()
    df['MASig'] = 0.0
    df.loc[df[PriceCol + '_MA'+str(MA1)] > df[PriceCol + '_MA'+str(MA2)], 'MASig'] = 1
    if LongOnly == False:
      df.loc[df[PriceCol + '_MA'+str(MA1)] < df[PriceCol + '_MA'+str(MA2)], 'MASig'] = -1
    if NoPriceCol:
      del df[PriceCol]
    return(df)


def GetLastSig(df, instrument = 'instrument', SigCol = 'MASig', ReturnDF = True):
    df1MA = df
    PriorSigDate = df1MA[df1MA[SigCol] != df1MA.iloc[-1][SigCol]].index[-1]
    PriorSig = df1MA[df1MA[SigCol] != df1MA.iloc[-1][SigCol]].iloc[-1][SigCol]
    LastSigChangeDate = df1MA.index[df1MA.index.get_loc(PriorSigDate) + 1]
    SigText1 = "The Current signal suggests to be " + "{:.2%}".format(df1MA.iloc[-1][SigCol]) + " invested."
    SigChgText = SigText1 + ' The signal last changed on ' + LastSigChangeDate.strftime('%Y-%m-%d') + ' from ' + str(PriorSig) + "x."
    #print(SigChgText)
    if ReturnDF:
        d = d = {'Instrument': instrument, 'Current Signal': [df1MA.iloc[-1][SigCol]], 'Signal Changed': [LastSigChangeDate.strftime('%Y-%m-%d')], 'Prior Sig.': [PriorSig]}
        temp = pd.DataFrame(data=d)
        return(temp)
    else:
        return(SigChgText)


def model(df):
    """Intakes df, returns str"""
    # TODO: TJ's logic here
    global sep
    SPX_with_BSGrowth = df[df['BS 12wk %Chg'] > 0]['SPX 1wk % chg'].shift()
    SPX_with_BSContraction = df[df['BS 12wk %Chg'] < 0]['SPX 1wk % chg'].shift()

    df1 = makeMASignals(df, 'Tot BS')

    merge_df = df.merge(df1, left_index=True, right_index=True)

    merge_merge_df['logret'] = np.log(merge_df['SPX']) - np.log(merge_df['SPX'].shift(1))
    merge_df['SPX_DD'] = merge_df['logret'].cumsum() - merge_df['logret'].cumsum().cummax()
    merge_df = merge_df.dropna()
    merge_df['NewSig'] = np.nan

    if merge_df.iloc[0]['MASig'] > 0 and merge_df.iloc[0]['SPX_DD'] < DD_to_buy:
        merge_df.at[merge_df.index[0], 'NewSig'] = merge_df.iloc[0]['MASig'] * 1.5
    else:
        merge_df.at[merge_df.index[0], 'NewSig'] = 0

    for count in range(1, merge_df.shape[0]):
        if merge_df.iloc[count]['MASig'] > 0 and merge_df.iloc[count]['SPX_DD'] < DD_to_buy:
            merge_df.at[merge_df.index[count], 'NewSig'] = merge_df.iloc[count]['MASig'] * 1.5
        elif merge_df.iloc[count]['MASig'] > 0 and merge_df.iloc[count - 1]['NewSig'] == 1.5:
            merge_df.at[merge_df.index[count], 'NewSig'] = merge_df.iloc[count]['MASig'] * 1.5
        elif merge_df.iloc[count]['MASig'] > 0:
            merge_df.at[merge_df.index[count], 'NewSig'] = merge_df.iloc[count]['MASig']
        else:
            merge_df.at[merge_df.index[count], 'NewSig'] = 0

    sigTextForUser = GetLastSig(df, 'SPX', SigCol='NewSig', ReturnDF=False)


    print('Since ' + df.index[0].strftime("%m-%d-%Y") + ' the performance of the model is a cumuluative return of: ')
    print("{:.0%}".format((df['logret'] * df['NewSig']).sum()) + ' with daily volatility of ' + "{:.0%}".format(
        (df['logret'] * df['NewSig']).std()))
    print('This compared to simply owning the S&P 500 with a return of:')
    print("{:.0%}".format((df['logret']).sum()) + ' with daily volatility of ' + "{:.0%}".format((df['logret']).std()))

    cum_perf_str = f"Since {merge_df.index[0].strftime('%m-%d-%Y')} the performance of the model is a cumuluative return of: {(merge_df['logret'] * merge_df['NewSig']).sum():.0 %} with daily volatility of {(merge_df['logret'] * merge_df['NewSig']).std():.0%}"

    out_str = f"""
    <h3>Fed watch signal</h3>
    <h4>{sigTextForUser}</h4>
    <br>
    <h4>Model Performance</h4>
    <p>{cum_perf_str}</p>
    <br>
    <h5>Long Term Rising Liquidity Stats</h5>
    <ul>
    <li>Mean when CBs Expanding: {SPX_with_BSGrowth.mean():.2%}</li>
    <li>Best Week: {SPX_with_BSGrowth.max():.2f}</li>
    <li>Worst Week: {SPX_with_BSGrowth.min():.2%}</li>
    </ul>
    
    <br>

    <h5>Contracting Liquidity Stats</h5>
    <ul>
    <li>Mean when CBs Contracting: {SPX_with_BSContraction.mean():.2%}</li>
    <li>Best Week: {SPX_with_BSContraction.max():.2f}</li>
    <li>Worst Week: {SPX_with_BSContraction.min():.2%}</li>
    </ul>
    """

    return out_str


def email_parse():
    """
    This stores emails in a list from the string input we get from the emails var.
    Can be replaced by a database as an enhancement.
    :return:  list: emails
    """
    email_list = [email.strip() for email in env['EMAILS'].split(",")]
    return email_list


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


def publish_message_ses(message, img_url):
    """
    Takes a list of emails and publishes and ses message to them.
    """

    SENDER = env['SENDER_EMAIL']

    # RECIPIENT = email

    # Set the AWS Region you're using for Amazon SES.
    AWS_REGION = "us-east-1"

    # The subject line for the email.
    SUBJECT = "FED WATCH"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = f"""
                \nFED WATCH CENTRAL BANK ANALYSIS\n
                \nFedwatch helps you monitor how central bank liquidty is affecting the macro economic environment.\n
                {message.replace("<br>", os.linesep).replace("<ul>","").replace("<li>","").replace("<h5>","")}
                """

    # The HTML body of the email.

    BODY_HTML = f"""<html>
                    <head></head>
                    <body>
                      <br>
                      <img src="{env['LOGO_URL']}" width=50%>
                      <br>
                      <h2>FED WATCH CENTRAL BANK ANALYSIS</h2>
                      <h5>Fedwatch helps you monitor how central bank liquidty is affecting the macro economic environment.</h5>
                      <br>

                      {message}
                      
                      <img src="{img_url}" width=50%>

                      <br>

                      <p>Disclaimer: This email is not an offer, solicitation of an offer, or advice to buy or sell securities. All investments involve risk and the past performance of a security, or financial product does not guarantee future results or returns. There is always the potential of losing money when you invest in securities, or other financial products. The data and other information used in generating the FedWatch email is not warranted as to completeness or accuracy and are subject to change without notice. Don’t trade with money you can’t afford to lose. Before acting on information in this email, you should consider whether it is suitable for your particular circumstances and strongly consider seeking advice from your own financial or investment adviser.</p>
                
                    </body>
                    </html>
                    """

    print(BODY_HTML)

    # The character encoding for the email.
    CHARSET = "UTF-8"

    client = boto3.client('ses', region_name=AWS_REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': email_parse(),
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # If you are not using a configuration set, comment or delete the following line
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def handler(event, context):
    """
    This function drives the AWS lambda.
    """
    global sep

    img_filename = create_filename("fed_watch_graph", "png")

    df = get_data()

    message = model(df)

    create_img(df['Tot BS'],
               df['SPX'],
               'Global Central Bank Liquidity \n Balance Sheets in USD',
               FileName=img_filename,
               title2='S&P 500',
               label1='Global liquidty in Trillions of USD')

    img_url = s3_upload(img_filename, dir='imgs')

    if env['MODE'].lower() == 'sns':
        publish_message_sns(message)
    else:
        message.replace(os.linesep, "<br>")
        publish_message_ses(message, img_url)

    return message