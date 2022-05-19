from common.secrets_mgr import get_secret
print(get_secret("sparknet-crpyt-key"))
print(get_secret("admin-ak"))
print(get_secret("admin-sak"))
print(type(get_secret("sparknet-crpyt-key")))
print(type(get_secret("admin-ak")))
print(str(get_secret("admin-ak")))
print(type(str(get_secret("admin-sak"))))