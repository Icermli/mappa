[![license: MIT](https://img.shields.io/badge/License-MIT-blue)](/LICENSE)
[![python](https://img.shields.io/badge/python-3.7-green.svg)]()
[![downloads](https://img.shields.io/github/downloads/Icermli/cereal/total.svg)]()
[![issues](https://img.shields.io/github/issues/Icermli/cereal.svg)](https://github.com/Icermli/cereal/issues)

# vappa

A minimalist explorer for V SYSTEMS.

### Installation


```
git clone https://github.com/Icermli/vappa.git
cd vappa
pip install requirements.txt
```

### Configurations

Configure the app via configuration files `config.json`:

```
# config.json

{
  "url": "http://3.16.244.131:9922",
  "address": ["AR4ukdxjymB9hGTK3zbYotMCzsYoe2e9hNY", "ARJetyPPUS4zTpCwf4a3mNUg2PguQAPwM8L", "AR4ogoRVfoKmgB9WiYgXDUn8awLcLo7NhNU",
              "ARNTCMHG1YYScGNzziUPxVBfEHiXdwW7AH9", "ARCvPwXnwbXgWUkkJdqaywUgE9aD9tFFR8z", "ARPYusJF86iqWASdPLSztY385Q3zinvVere",
              "ARAqjEYo4Wt7syabu6dagbn35LnPeeMbVHD", "AR4fokH6xA3xqVb62VxWsZm8ZFMkZwU7RqA", "AR7qbCQYYt69QGu8GwDV7fud2kQu1Ysm6dz",
              "ARC2M9kTRR2jityXVN74UTtvSGr2SZAX3dK", "ARCEAR2jicAnXRUQ4znYtcCPPMmwoScpPyw", "ARPvCd79J1papTaKXN3U9uDmB4DcfHsmJRR",
              "ARBUgFyHUW8N56TEvWm6RNerhearPCrxRfX", "ARAUs5x2zvUsRRXJn6SJGSzE1uLKPAF719f", "ARBg7V87g6VdDDBpHS4ZQaLDkMTSZU2RFF1"],
  "supernode": ["V Power", "OKEx Pool", "Staking KOREA", "Bullseye.io", "HelloVSYS", "Huobi Pool", "Black Diamond", "Bullseye.io",
                "Bullseye.io", "Primecoin", "Cobo Wallet", "V Sydney", "Bullseye.io", "Blockchain keys", "Bullseye.io"]
}
```

Test locally:

```
python app.py
```

By default the test server is located at `http://localhost:5000/`.
