# %%
import yagmail

# %%
c = {"user": "ops_support@reconnectenergy.com", "password": "opsfs123@"}
acc = yagmail.SMTP(**c)
acc.login()
# %%

_to = "atindra.nair@reconnectenergy.com"
_subject = "IMDAS_SCADA_DIFFERENCES []"

# %%

im1 = yagmail.inline("./plots/10:30_to_14:15/SS00249_Achampet_AB.jpg")
im2 = yagmail.inline("./plots/10:30_to_14:15/SS00282_Katkol.jpg")
cntnts = ["HI", im1, im2, "DONE"]
acc.send(to=_to, subject=_subject, contents=cntnts)
# %%
