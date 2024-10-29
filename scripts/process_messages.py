import numpy as np
import pandas as pd
import pendulum as plm
import os
import humanfriendly as hf
import io
import re

############################
############################
#  JH MESSAGES PROCESSOR   #
############################
############################
def jh_token_processing(tokens):
    """
    Process a jupyterhub vector of string tokens
    """
    length = len(tokens)

    if length < 15:
        tokens_ = ["" for  i in range(15)]
        tokens_[:5] = [tokens[i] for i in range(5)]
        tokens_[5:] = ["NA" for i in range(10)]
        tokens_[4]  = tokens[4][:-1] # rm : after jupyterhub[xxxx]
        tokens_[13] = "@NA"
        # tokens with length == 5 are empty, but if length > 5 then we retrieve the message and save it
        if length > 5: tokens_[12] = " ".join(tokens[5:])
        return tokens_
    # end if

    tokens_ = tokens.copy()
    tokens_[4] = tokens[4][:-1] # rm : after jupyterhub[xxxx]

    if length == 15:
        tokens_[5] = tokens[5][1:]     # rm [ before I,W, or E
        tokens_[9] = tokens[9][:-1]    # rm ] after message
        tokens_[13] = tokens[13][1:-1] # rm ( before and ) after line
        tokens_[14] = tokens[14][:-2]  # rm 'ms' after number
        return tokens_
    # end if

    if length == 17:
        tokens_[5] = tokens[5][1:]                # rm [ before I,W, or E
        tokens_[9] = tokens[9][:-1]               # rm ] after message
        if tokens[13] == "->": # these contain IP address, user, and time - we save those
            tokens_[12] = " ".join(tokens[12:15]) # join directories
            tokens_[15] = tokens[15][1:-1]        # rm ( before and ) after line
            tokens_[16] = tokens[16][:-2]         # rm 'ms' after number
            tokens_ = tokens_[:13]+tokens_[15:17] # rm directories that were joined
        else: # these only contain a message, which we retrieve but leave the rest as NA
            tokens_[10] = "NA"                    # HTTP code 1
            tokens_[11] = "NA"                    # HTTP code 2
            tokens_[12] = " ".join(tokens[10:])   # join directories
            tokens_[13] = "@NA"                   # user and IP (@ necessary for post-processing)
            tokens_[14] = "NA"                    # add missing time
            tokens_ = tokens_[:15]
        # end if
        return tokens_
    # end if

    if length != 15 or length != 18:
        tokens_[5] = tokens[5][1:]          # rm [ before I,W, or E
        tokens_[9] = tokens[9][:-1]         # rm ] after message
        tokens_[10] = "NA"                  # HTTP code 1
        tokens_[11] = "NA"                  # HTTP code 2
        tokens_[12] = " ".join(tokens[10:]) # join directories
        tokens_[13] = "@NA"                 # user and IP (@ necessary for post-processing)
        tokens_[14] = "NA"                  # add missing time
        tokens_ = tokens_[:15]              # keep relevant bits only
        return tokens_
    # end if

    return tokens_
########################################
########################################
########################################
########################################


############################
############################
#   PROCESS JH MESSAGES    #
############################
############################
logfiles = sorted(os.listdir("messages"))
itr = 0
for lf in logfiles:
    if lf[:8] != 'messages': continue # ignore DS store and similar non-messages files
    fn = os.path.join("messages", lf)

    print(f"Filling messages_{itr:03}.csv -- Processing {fn}")
    # read with whitespace delim; standardize columns / whitespace with "
    with open(fn, "r", encoding='utf-8', errors='ignore') as f:
        fo = io.StringIO()
        data = f.readlines()

        # find where the message is and surround with quotes
        data2 = []
        for line in data:
            tokens = line.replace('"', '').split()

            # filter lines to keep only messages with info
            if "jupyterhub" not in tokens[4]: continue

            # process tokens
            tokens = jh_token_processing(tokens = tokens)

            # join
            data2.append( " ".join(tokens[:12]) + ' "' + "".join(tokens[12]) + '" ' + " ".join(tokens[13:]) +'\n' )
        # end for

        if len(data2) == 0:
            print(f"No data found in {fn}; skipping")
            continue
        # end if

        fo.writelines(data2)
        fo.seek(0)
    # end with

    # read the table and rename the columns
    df_tmp = pd.read_table(fo, delim_whitespace=True, header=None)

    df_tmp.columns = ["month", "day", "timestamp", "host", "service", "msg_type", "date", "timestamp2", "service2", "origin", "request_no", "request_type", "directory", "user_IP", "time"]

    # get times
    df_tmp[["hour", "minute", "second"]] = df_tmp["timestamp"].str.strip("[").str.strip("]").str.split(":", expand=True)

    # get date
    df_tmp["year"] = lf[9:13]
    df_tmp["month"] = df_tmp["month"].map({"Jan" : 1, "Feb" : 2, "Mar" : 3, "Apr" : 4, "May" : 5, "Jun" : 6, "Jul" : 7, "Aug" : 8, "Sep" : 9, "Oct" : 10, "Nov" : 11, "Dec" : 12})

    # remove unused columns
    df_tmp = df_tmp[["year", "month", "day", "hour", "minute", "second", "service", "msg_type", "origin", "request_no", "request_type", "directory", "user_IP"]]

    # output to file
    df_tmp.to_csv(f"processed/jh_messages_{itr:03}.csv", index=False)
    itr += 1
# end for
print('Done!')
