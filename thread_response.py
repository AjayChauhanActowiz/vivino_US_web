from curl_cffi import requests
# import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


cookies = {
    '_vvno_rdt_cid': 'deleted',
    '_vvno_rdt_cid': 'deleted',
    'client_cache_key': 'WqKMSntByniKi937fKVpQu2QX45DBOcIa%2FiH1Wx1Sj6V2D9mZz7fpi6Bi7bB32MmpQL7MxYxFcyAee8zc%2FeqKPXDfk8SUDEcuoBbhQvOaEYdMqBIF32eeIviag4zwDLhke3TGrYkR7vZz0KnhUAkoSiN--nz%2FVgk89h4BgwvMB--3xMOzJL40iecXhR6OrVyjw%3D%3D',
    'first_time_visit': 'YsscrVz%2BA%2F%2FLDIJiS3DofZDcKWvGjDH%2Fl0IXzLb66bVjJIbEiW52gy47bNemhylGWmRLYMf1P7vOnS94e%2F%2BKv%2BW9ScnhlHiQHEvBlsY4IR3j%2FobYHOH9fIME5hrRccrr8Cs%3D--CJiHOrkay1IcZMXp--vMrtTp6HPRvgY7y%2BIX7ZKA%3D%3D',
    'anonymous_tracking_id': 'W2bNxrwu14EVxwsAHkhE5eXj7WQYFvwjz68Aig%2BVwYkLpDl4hyB6%2FgTpuHHqKHShCumOTxe5q4zYcdHPtUXieSA6qtBwpuREb9P57BtWW454XXKUTR0vEQ3ES0BBY99W00j8IAL6xe29S3%2FLzvsGeE5dVaELMG8NAX4GR1osXpgeBTZAUcg4CH6XXjCuzVZ%2FspLI--HLCM5mVIdZzki5%2FS--zVpy%2FKCXMmpt2lA5IlJDIQ%3D%3D',
    'eeny_meeny_explorer_card_v4': 'omD%2BSYij9Rfzxhuvv%2B69E0NGxbriBklUKzHe%2BKGbcRk54nySRba%2BxWpRfeaN3sImjBu8c1L0DnpXjEcRJGBU3Q%3D%3D',
    'eeny_meeny_explorer_facets_v1': '1yb3Cv79a37Rqmhhz8Hjq%2FojMM7UvhJ9JC2OnYADS%2BMMzaq5lRSzl037Utmrgb1ayj1sx1GEUA%2Bc8%2BvwI%2FQcRA%3D%3D',
    'eeny_meeny_frontpage_banner_v1': 'P2Za1HgVc7s6dKpHQ3SgT66oSJJyjnRorcPFHBnTtrok5DwG0d4gCZThI8vq1t4nkCZaN%2B1GyqpxNm2LFps1PA%3D%3D',
    'eeny_meeny_for_you_4_onoffer_bands_v1': 'znsJrFeHknRszR1qwRc1gaiShjbuk8PEfcIrpwaIMNJp8SKTLBClyaFAUwQIV%2FV5k4Na4O7QIYkrwgycWnaxDw%3D%3D',
    'eeny_meeny_web_cart_dropdown_enabled_v1': 'Zp0byTk%2BKKBYypy9DsL0VrWSGboY1WIYH02GL1hG0v5A0lDTUVEjufiYlQAtx9C3NGwCLunjkVrHS2btuf3Caw%3D%3D',
    'mp_bee7544764ece4336acb3b402265c80c_mixpanel': '%7B%22distinct_id%22%3A%22%24device%3Ac5c54c3d-78ff-4c6e-8163-3494c23e453b%22%2C%22%24device_id%22%3A%22c5c54c3d-78ff-4c6e-8163-3494c23e453b%22%2C%22%24search_engine%22%3A%22google%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.google.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.google.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D',
    '_gcl_au': '1.1.1242759235.1757483064',
    'ab.storage.deviceId.932da152-bffc-4d62-a719-894efe56ebac': 'g%3Ab994c1c0-f7bc-ea17-a7c1-57d313804c87%7Ce%3Aundefined%7Cc%3A1757483063855%7Cl%3A1757483063855',
    '__attentive_id': '1b7ba1e1189047cc9fb84662529a6659',
    '__attentive_session_id': 'a5ea4ef2920a4ddbb47e5590ab6905dc',
    '__attentive_cco': '1757483064520',
    '_ga': 'GA1.1.778395768.1757483065',
    '_fbp': 'fb.1.1757483064970.641831850311017458',
    '_pin_unauth': 'dWlkPU5XVmlPV1l3TVdFdE4yUmlZeTAwTUdKaUxUazBZVEF0TXpoaFptSXdaRGczTjJOaA',
    '__attentive_dv': '1',
    '__attentive_ss_referrer': 'https://www.google.com/',
    'FPID': 'FPID2.2.ni3WSZEVpnVWM1NmHOP282h3HaztnQ%2BAWTiK6hm4w8M%3D.1757483065',
    'FPLC': 'Z%2BZeq1YLfvnjuQ87mvlwcQWWIHID%2Bsbv%2F%2FT0CD2FpGBVi6kuAEZWmjnctwcASUdssO1w%2Bzo7leMGouVnKgCpLp1n4TYpYd5NBLfTPZGx5XCK6VAk8O5u1tc3F%2FGzvA%3D%3D',
    '__gads': 'ID=b340e6a0e69745ca:T=1757483080:RT=1757483080:S=ALNI_Mak3NOPi5qNOekszTI3Y0UN_l2vfA',
    '__gpi': 'UID=000011941577a744:T=1757483080:RT=1757483080:S=ALNI_MazhcqtuNHYKCbExjb8DInBslnCMA',
    '__eoi': 'ID=6530ce038b548a42:T=1757483080:RT=1757483080:S=AA-Afjb6stZXPLdJovIwBcOLc37C',
    '_vvno_rdt_cid': 'deleted',
    'recently_viewed': '2hu3Xj33P0OwjVDuBA3fabuPJ2aqoKonWTnUa9Rp%2FHQcTvBc2HIL3a%2BVMY5XO6qpH4h%2FoslWJvTXYlgUAR%2FZNTbaqll2Dv2A6%2FIas1QloyBl5RDOPtkc%2F02uCVGEKGLEBsKcv5C3jGSDFjAW71YuRVGLu0N%2F1%2Bx96RXZtqTBAlmT7QpdHdh5yjeJb9ybpUSen03fezN83EYTbFm5U7boKcfsNu1Y4ALSekWnVYo0RBjnkdLPQoEkQTNnImwuw57Isk5pTGe8bq11E%2BlPnT3fhJYpvHG%2B2c0tsstvpATU6FJ%2Fye4ZlX5PB8bVRc5%2BenV1ryWbCmc0nhr3xZxv9%2FCIH1AvV5%2FiSq3FUi3bgUE2mffeCWfhVCkFplhMSdzUlreQ5qWsBHFaflzhEpdXK9yiZ%2FygS0cX3CR0GgPCSd5AgZpZIUYL6o%2FZnnhcNY01Q64Fwd%2FTSLngiC73N%2B%2BjnejD%2BYfDpaw6yrjfguIxQmsSCWMVr9XoiSD0fji82d9uPb2v%2FNHmIIza2OIogX0uCbU88sMAb7GF--0OE4tCcfx4TT0dHX--8E6e5o8GDzq2dE3K0Y%2B0OA%3D%3D',
    'OptanonConsent': 'isGpcEnabled=0&datestamp=Wed+Sep+10+2025+11%3A16%3A11+GMT%2B0530+(India+Standard+Time)&version=202505.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=bbf337eb-a7cd-4888-a820-21247d5547ad&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0%2CC0002%3A0&AwaitingReconsent=false',
    'ab.storage.sessionId.932da152-bffc-4d62-a719-894efe56ebac': 'g%3A1e62837b-96dd-bef6-1c55-c12945e3899e%7Ce%3A1757484971728%7Cc%3A1757483063853%7Cl%3A1757483171728',
    '_ga_D35SJB5ZNL': 'GS2.1.s1757483064$o1$g1$t1757483171$j50$l0$h0',
    '_ga_000000000': 'GS2.1.s1757483064$o1$g1$t1757483171$j50$l0$h1297326174',
    '_rdt_uuid': '1757483064263.d1492c61-0446-4aff-8ac1-e7deabd9ee23',
    '_attn_': 'eyJ1Ijoie1wiY29cIjoxNzU3NDgzMDY0NTE5LFwidW9cIjoxNzU3NDgzMDY0NTE5LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcIjFiN2JhMWUxMTg5MDQ3Y2M5ZmI4NDY2MjUyOWE2NjU5XCJ9Iiwic2VzIjoie1widmFsXCI6XCJhNWVhNGVmMjkyMGE0ZGRiYjQ3ZTU1OTBhYjY5MDVkY1wiLFwidW9cIjoxNzU3NDgzMTcyMDgyLFwiY29cIjoxNzU3NDgzMTcyMDgyLFwibWFcIjowLjAyMDgzMzMzMzMzMzMzMzMzMn0ifQ==',
    '__attentive_pv': '5',
    'aws-waf-token': '5e02045e-fb24-46b1-ba54-87c982c2da11:EQoAgjcoZeAPAAAA:1KeWQqU8Ivl/48LD5HHH0bbSDcUBJYjXrX7Z/sHnzOLWIBz2i8y+u3jq81ryoS6SoOXF6oi6+eE21QE27mqiPW5/8lguZ90CUJEACh9N+xE8WHFSJG2phjZ7VDaFDNnZ3Zs2VLBpGkJX4ldPH6IpcHWJKqmdlT8V2UkeeijnRgEHJchOYtG+RzSM1VHjkgbQUl3yHQxHKbfe9oNqcS6POkqUR3Qfw9+jZPyQkMABMI45hZh2NK2PfLnddM4i0K4/ysAiZBM=',
    'csrf_token': 'MR_8VhxXTYZMUTMG9ekE05j-_wB6WFxfpGa9pb6L_BJnBkNbSaDU-9utfIyHZtbJZnaZOTzMthsD3Dh0eoUa-g',
    '_ruby-web_session': 'i9nRwUw5d6XZiP9bBdtM3nIRCoST6q9p0aBF8C3gQcEeOQj3ng8b94ai%2FocoM4hl5txD0Z4IjamSq6wgvYevcTKhIx3%2FI0an3B4DzV8Wmwvt7ehTImRoBORWWTSyWQMFUHkhdnPzjeGOYCtk592UwF6HbdbCfl%2B%2FK70BuetcYwt3JsFonLfJnwYrvlsaU6i3qNrWympVf3I7GWot0zmwG3YlEOzZFnGNgPu%2FL6kWb1WVztH1e1HtSZiLm1uK8dOmXHWiTPbQrQxX6iBnuDHrRr0MHhMoUp6Y2diXLw%2Bremq3EZLfqeOx3KCZtQk%2FpZBjTF4C2bslODeZKImicpBNpUdPtCEDwBTnxHqBx1oc8rH9%2BNsN2Glk5DkpTnI%2BgXC4vTPvw1Mx6L86ouM2ZlHgi7nAtZOPffEA%2FPqMxv1ixM%2FRI2x3vHhovPAyXmUiUp7JjboqixkeD%2FEKPiLeKnNmUGpnYlSDItNry9R9aIfzcsQngl%2F4ajWyQ97VVjyeUte8Hzca7gq%2BkB2Ha0jQBPnb9AvqlSOI4%2FnpFodLeeAcq8IcO87X%2BwlF4G3vUQ%3D%3D--bKj%2BOKuHFUtC9DPG--tAFbJJwZGrdttExaI8wTFg%3D%3D',
    '_dd_s': 'rum=0&expire=1757484101542',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'max-age=0',
    'if-none-match': 'W/"42ec0db6d3d19be6070706fdb59190e9"',
    'priority': 'u=0, i',
    'referer': 'https://www.vivino.com/macauley-vineyard-cabernet-sauvignon/w/88661?year=2022&price_id=39576233',
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
    # 'cookie': '_vvno_rdt_cid=deleted; _vvno_rdt_cid=deleted; client_cache_key=WqKMSntByniKi937fKVpQu2QX45DBOcIa%2FiH1Wx1Sj6V2D9mZz7fpi6Bi7bB32MmpQL7MxYxFcyAee8zc%2FeqKPXDfk8SUDEcuoBbhQvOaEYdMqBIF32eeIviag4zwDLhke3TGrYkR7vZz0KnhUAkoSiN--nz%2FVgk89h4BgwvMB--3xMOzJL40iecXhR6OrVyjw%3D%3D; first_time_visit=YsscrVz%2BA%2F%2FLDIJiS3DofZDcKWvGjDH%2Fl0IXzLb66bVjJIbEiW52gy47bNemhylGWmRLYMf1P7vOnS94e%2F%2BKv%2BW9ScnhlHiQHEvBlsY4IR3j%2FobYHOH9fIME5hrRccrr8Cs%3D--CJiHOrkay1IcZMXp--vMrtTp6HPRvgY7y%2BIX7ZKA%3D%3D; anonymous_tracking_id=W2bNxrwu14EVxwsAHkhE5eXj7WQYFvwjz68Aig%2BVwYkLpDl4hyB6%2FgTpuHHqKHShCumOTxe5q4zYcdHPtUXieSA6qtBwpuREb9P57BtWW454XXKUTR0vEQ3ES0BBY99W00j8IAL6xe29S3%2FLzvsGeE5dVaELMG8NAX4GR1osXpgeBTZAUcg4CH6XXjCuzVZ%2FspLI--HLCM5mVIdZzki5%2FS--zVpy%2FKCXMmpt2lA5IlJDIQ%3D%3D; eeny_meeny_explorer_card_v4=omD%2BSYij9Rfzxhuvv%2B69E0NGxbriBklUKzHe%2BKGbcRk54nySRba%2BxWpRfeaN3sImjBu8c1L0DnpXjEcRJGBU3Q%3D%3D; eeny_meeny_explorer_facets_v1=1yb3Cv79a37Rqmhhz8Hjq%2FojMM7UvhJ9JC2OnYADS%2BMMzaq5lRSzl037Utmrgb1ayj1sx1GEUA%2Bc8%2BvwI%2FQcRA%3D%3D; eeny_meeny_frontpage_banner_v1=P2Za1HgVc7s6dKpHQ3SgT66oSJJyjnRorcPFHBnTtrok5DwG0d4gCZThI8vq1t4nkCZaN%2B1GyqpxNm2LFps1PA%3D%3D; eeny_meeny_for_you_4_onoffer_bands_v1=znsJrFeHknRszR1qwRc1gaiShjbuk8PEfcIrpwaIMNJp8SKTLBClyaFAUwQIV%2FV5k4Na4O7QIYkrwgycWnaxDw%3D%3D; eeny_meeny_web_cart_dropdown_enabled_v1=Zp0byTk%2BKKBYypy9DsL0VrWSGboY1WIYH02GL1hG0v5A0lDTUVEjufiYlQAtx9C3NGwCLunjkVrHS2btuf3Caw%3D%3D; mp_bee7544764ece4336acb3b402265c80c_mixpanel=%7B%22distinct_id%22%3A%22%24device%3Ac5c54c3d-78ff-4c6e-8163-3494c23e453b%22%2C%22%24device_id%22%3A%22c5c54c3d-78ff-4c6e-8163-3494c23e453b%22%2C%22%24search_engine%22%3A%22google%22%2C%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.google.com%22%2C%22__mps%22%3A%7B%7D%2C%22__mpso%22%3A%7B%22%24initial_referrer%22%3A%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%22www.google.com%22%7D%2C%22__mpus%22%3A%7B%7D%2C%22__mpa%22%3A%7B%7D%2C%22__mpu%22%3A%7B%7D%2C%22__mpr%22%3A%5B%5D%2C%22__mpap%22%3A%5B%5D%7D; _gcl_au=1.1.1242759235.1757483064; ab.storage.deviceId.932da152-bffc-4d62-a719-894efe56ebac=g%3Ab994c1c0-f7bc-ea17-a7c1-57d313804c87%7Ce%3Aundefined%7Cc%3A1757483063855%7Cl%3A1757483063855; __attentive_id=1b7ba1e1189047cc9fb84662529a6659; __attentive_session_id=a5ea4ef2920a4ddbb47e5590ab6905dc; __attentive_cco=1757483064520; _ga=GA1.1.778395768.1757483065; _fbp=fb.1.1757483064970.641831850311017458; _pin_unauth=dWlkPU5XVmlPV1l3TVdFdE4yUmlZeTAwTUdKaUxUazBZVEF0TXpoaFptSXdaRGczTjJOaA; __attentive_dv=1; __attentive_ss_referrer=https://www.google.com/; FPID=FPID2.2.ni3WSZEVpnVWM1NmHOP282h3HaztnQ%2BAWTiK6hm4w8M%3D.1757483065; FPLC=Z%2BZeq1YLfvnjuQ87mvlwcQWWIHID%2Bsbv%2F%2FT0CD2FpGBVi6kuAEZWmjnctwcASUdssO1w%2Bzo7leMGouVnKgCpLp1n4TYpYd5NBLfTPZGx5XCK6VAk8O5u1tc3F%2FGzvA%3D%3D; __gads=ID=b340e6a0e69745ca:T=1757483080:RT=1757483080:S=ALNI_Mak3NOPi5qNOekszTI3Y0UN_l2vfA; __gpi=UID=000011941577a744:T=1757483080:RT=1757483080:S=ALNI_MazhcqtuNHYKCbExjb8DInBslnCMA; __eoi=ID=6530ce038b548a42:T=1757483080:RT=1757483080:S=AA-Afjb6stZXPLdJovIwBcOLc37C; _vvno_rdt_cid=deleted; recently_viewed=2hu3Xj33P0OwjVDuBA3fabuPJ2aqoKonWTnUa9Rp%2FHQcTvBc2HIL3a%2BVMY5XO6qpH4h%2FoslWJvTXYlgUAR%2FZNTbaqll2Dv2A6%2FIas1QloyBl5RDOPtkc%2F02uCVGEKGLEBsKcv5C3jGSDFjAW71YuRVGLu0N%2F1%2Bx96RXZtqTBAlmT7QpdHdh5yjeJb9ybpUSen03fezN83EYTbFm5U7boKcfsNu1Y4ALSekWnVYo0RBjnkdLPQoEkQTNnImwuw57Isk5pTGe8bq11E%2BlPnT3fhJYpvHG%2B2c0tsstvpATU6FJ%2Fye4ZlX5PB8bVRc5%2BenV1ryWbCmc0nhr3xZxv9%2FCIH1AvV5%2FiSq3FUi3bgUE2mffeCWfhVCkFplhMSdzUlreQ5qWsBHFaflzhEpdXK9yiZ%2FygS0cX3CR0GgPCSd5AgZpZIUYL6o%2FZnnhcNY01Q64Fwd%2FTSLngiC73N%2B%2BjnejD%2BYfDpaw6yrjfguIxQmsSCWMVr9XoiSD0fji82d9uPb2v%2FNHmIIza2OIogX0uCbU88sMAb7GF--0OE4tCcfx4TT0dHX--8E6e5o8GDzq2dE3K0Y%2B0OA%3D%3D; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Sep+10+2025+11%3A16%3A11+GMT%2B0530+(India+Standard+Time)&version=202505.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=bbf337eb-a7cd-4888-a820-21247d5547ad&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0003%3A0%2CC0001%3A1%2CC0004%3A0%2CC0002%3A0&AwaitingReconsent=false; ab.storage.sessionId.932da152-bffc-4d62-a719-894efe56ebac=g%3A1e62837b-96dd-bef6-1c55-c12945e3899e%7Ce%3A1757484971728%7Cc%3A1757483063853%7Cl%3A1757483171728; _ga_D35SJB5ZNL=GS2.1.s1757483064$o1$g1$t1757483171$j50$l0$h0; _ga_000000000=GS2.1.s1757483064$o1$g1$t1757483171$j50$l0$h1297326174; _rdt_uuid=1757483064263.d1492c61-0446-4aff-8ac1-e7deabd9ee23; _attn_=eyJ1Ijoie1wiY29cIjoxNzU3NDgzMDY0NTE5LFwidW9cIjoxNzU3NDgzMDY0NTE5LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcIjFiN2JhMWUxMTg5MDQ3Y2M5ZmI4NDY2MjUyOWE2NjU5XCJ9Iiwic2VzIjoie1widmFsXCI6XCJhNWVhNGVmMjkyMGE0ZGRiYjQ3ZTU1OTBhYjY5MDVkY1wiLFwidW9cIjoxNzU3NDgzMTcyMDgyLFwiY29cIjoxNzU3NDgzMTcyMDgyLFwibWFcIjowLjAyMDgzMzMzMzMzMzMzMzMzMn0ifQ==; __attentive_pv=5; aws-waf-token=5e02045e-fb24-46b1-ba54-87c982c2da11:EQoAgjcoZeAPAAAA:1KeWQqU8Ivl/48LD5HHH0bbSDcUBJYjXrX7Z/sHnzOLWIBz2i8y+u3jq81ryoS6SoOXF6oi6+eE21QE27mqiPW5/8lguZ90CUJEACh9N+xE8WHFSJG2phjZ7VDaFDNnZ3Zs2VLBpGkJX4ldPH6IpcHWJKqmdlT8V2UkeeijnRgEHJchOYtG+RzSM1VHjkgbQUl3yHQxHKbfe9oNqcS6POkqUR3Qfw9+jZPyQkMABMI45hZh2NK2PfLnddM4i0K4/ysAiZBM=; csrf_token=MR_8VhxXTYZMUTMG9ekE05j-_wB6WFxfpGa9pb6L_BJnBkNbSaDU-9utfIyHZtbJZnaZOTzMthsD3Dh0eoUa-g; _ruby-web_session=i9nRwUw5d6XZiP9bBdtM3nIRCoST6q9p0aBF8C3gQcEeOQj3ng8b94ai%2FocoM4hl5txD0Z4IjamSq6wgvYevcTKhIx3%2FI0an3B4DzV8Wmwvt7ehTImRoBORWWTSyWQMFUHkhdnPzjeGOYCtk592UwF6HbdbCfl%2B%2FK70BuetcYwt3JsFonLfJnwYrvlsaU6i3qNrWympVf3I7GWot0zmwG3YlEOzZFnGNgPu%2FL6kWb1WVztH1e1HtSZiLm1uK8dOmXHWiTPbQrQxX6iBnuDHrRr0MHhMoUp6Y2diXLw%2Bremq3EZLfqeOx3KCZtQk%2FpZBjTF4C2bslODeZKImicpBNpUdPtCEDwBTnxHqBx1oc8rH9%2BNsN2Glk5DkpTnI%2BgXC4vTPvw1Mx6L86ouM2ZlHgi7nAtZOPffEA%2FPqMxv1ixM%2FRI2x3vHhovPAyXmUiUp7JjboqixkeD%2FEKPiLeKnNmUGpnYlSDItNry9R9aIfzcsQngl%2F4ajWyQ97VVjyeUte8Hzca7gq%2BkB2Ha0jQBPnb9AvqlSOI4%2FnpFodLeeAcq8IcO87X%2BwlF4G3vUQ%3D%3D--bKj%2BOKuHFUtC9DPG--tAFbJJwZGrdttExaI8wTFg%3D%3D; _dd_s=rum=0&expire=1757484101542',
}

params = {
    'year': '2022',
    'price_id': '39576233',
}

# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# token = "f42a5b59aec3467e97a8794c611c436b91589634343"
# proxyModeUrl = "http://{}:@proxy.scrape.do:8080".format(token)
# proxies = {
#     "http": proxyModeUrl,
#     "https": proxyModeUrl,
# }
# scraper_api_token = '3cab9fca116e97dc833efb1b6464c996'
# proxies = {
#     "http": f"http://scraperapi:{scraper_api_token}@proxy-server.scraperapi.com:8001",
#     "https": f"http://scraperapi:{scraper_api_token}@proxy-server.scraperapi.com:8001"
# }
def response_check(start_iteration, num_requests):
    """Perform multiple requests inside one thread to reduce overhead."""
    batch_results = []
    for i in range(num_requests):
        iteration = start_iteration + i
        st = time.time()
        try:
            response = requests.get(
                'https://www.vivino.com/US/en/macauley-vineyard-cabernet-sauvignon/w/88661',
                params=params,
                # headers=headers,
                # cookies=cookies,
                impersonate='chrome120',
                # proxies=proxies,
                # verify=False,
                timeout=120
            )
            if fr'49.99' in response.text and fr'macauley-vineyard-cabernet-sauvignon-2022' in response.text:
                return_dict = {
                    'iteration': iteration,
                    'status': response.status_code,
                    'response': 'good',
                    'time_taken': time.time()-st
                }
                batch_results.append(return_dict)
                print(return_dict)
            else:
                return_dict = {
                    'iteration': iteration,
                    'status': response.status_code,
                    'response': 'bad',
                    'time_taken': time.time() - st
                }
                batch_results.append(return_dict)
                print(return_dict)
        except Exception as e:
            return_dict = {
                'iteration': iteration,
                'status': None,
                'response': f'error: {e}',
                'time_taken': time.time() - st
            }
            batch_results.append(return_dict)
            print(return_dict)
    return batch_results

results = []
thread_count = 20
total_requests = 3000
requests_per_thread = 10  # Each worker handles 10 requests

with ThreadPoolExecutor(max_workers=thread_count) as executor:
    futures = []
    for start in range(1, total_requests + 1, requests_per_thread):
        futures.append(executor.submit(response_check, start, requests_per_thread))

    for future in as_completed(futures):
        batch = future.result()
        for result in batch:
            # print(result)
            results.append(result)

# Save results to Excel
file_name = 'vivino_us_pdp_feasibility_test'
df = pd.DataFrame(results)
df.to_excel(f'{file_name}.xlsx', index=False)
print(f"Results saved to {file_name}.xlsx")


