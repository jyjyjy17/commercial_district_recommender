import pandas as pd
from data.data_loader import load_data
import os
# 현재 스크립트의 디렉토리 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# CSV 파일 경로 설정
RESIDENT = os.path.join(BASE_DIR, "./서울시 상권분석서비스(상주인구-상권).csv")
FOOT_TRAFFIC = os.path.join(BASE_DIR, "./서울시 상권분석서비스(길단위인구-상권).csv")
SALES = os.path.join(BASE_DIR, "./서울시 상권분석서비스(추정매출-상권).csv")
CHANGE_INDEX = os.path.join(BASE_DIR,"./서울시 상권분석서비스(상권변화지표-상권).csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "./merged_data_2024_1.csv")


def preprocess_data():
    resident_data = load_data(RESIDENT)
    foot_trafic_data = load_data(FOOT_TRAFFIC)
    sales_data = load_data(SALES)
    change_index_data = load_data(CHANGE_INDEX)
    # 기준년분기 코드와 상권 구분 코드 필터링 조건
    target_quarters = [20241]
    target_business_code = 'A'
    target_service_codes = ['CS100001', 'CS100008', 'CS100009', 'CS100003', 'CS100004',
                            'CS100005', 'CS100006', 'CS100007', 'CS100010']

    resident_columns = ['기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명',
                           '총_상주인구_수','연령대_10_상주인구_수', '연령대_20_상주인구_수', '연령대_30_상주인구_수',
        '연령대_40_상주인구_수', '연령대_50_상주인구_수', '연령대_60_이상_상주인구_수']

    foot_trafic_columns = [
        '기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명',
        '총_유동인구_수',
        '연령대_10_유동인구_수', '연령대_20_유동인구_수', '연령대_30_유동인구_수',
        '연령대_40_유동인구_수', '연령대_50_유동인구_수', '연령대_60_이상_유동인구_수',
        '시간대_00_06_유동인구_수', '시간대_06_11_유동인구_수', '시간대_11_14_유동인구_수',
        '시간대_14_17_유동인구_수', '시간대_17_21_유동인구_수', '시간대_21_24_유동인구_수',
        '월요일_유동인구_수', '화요일_유동인구_수', '수요일_유동인구_수',
        '목요일_유동인구_수', '금요일_유동인구_수', '토요일_유동인구_수', '일요일_유동인구_수'
    ]
    sales_columns = [
        '기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명',
        '당월_매출_금액', '당월_매출_건수', '주중_매출_금액', '주말_매출_금액',
        '월요일_매출_금액', '화요일_매출_금액', '수요일_매출_금액', '목요일_매출_금액',
        '금요일_매출_금액', '토요일_매출_금액', '일요일_매출_금액',
        '시간대_00~06_매출_금액', '시간대_06~11_매출_금액', '시간대_11~14_매출_금액',
        '시간대_14~17_매출_금액', '시간대_17~21_매출_금액', '시간대_21~24_매출_금액',
        '남성_매출_금액', '여성_매출_금액', '연령대_10_매출_금액', '연령대_20_매출_금액',
        '연령대_30_매출_금액', '연령대_40_매출_금액', '연령대_50_매출_금액',
        '연령대_60_이상_매출_금액'
    ]
    change_index_columns = [
        '기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명',
        '상권_변화_지표'
    ]
    # 상권 코드로 데이터를 합치기 위해 공통된 키 설정
    join_keys = ['기준_년분기_코드', '상권_코드']

    # 필터링 1단계. 필요한 열만 선택
    filtered_resident_data = resident_data[resident_columns]
    filtered_foot_trafic = foot_trafic_data[foot_trafic_columns]
    filtered_sales_data = sales_data[sales_columns]
    filtered_change_index = change_index_data[change_index_columns]

    # 필터링 2단계. 데이터 필터링
    filtered_resident_data = filtered_resident_data[
        (filtered_resident_data['기준_년분기_코드'].isin(target_quarters)) &
        (filtered_resident_data['상권_구분_코드'] == target_business_code)
        ]
    filtered_foot_trafic = filtered_foot_trafic[
        (filtered_foot_trafic['기준_년분기_코드'].isin(target_quarters)) &
        (filtered_foot_trafic['상권_구분_코드'] == target_business_code)
        ]
    filtered_change_index = filtered_change_index[
        (filtered_change_index['기준_년분기_코드'].isin(target_quarters)) &
        (filtered_change_index['상권_구분_코드'] == target_business_code)
        ]
    filtered_sales_data = filtered_sales_data[
        (sales_data['기준_년분기_코드'].isin(target_quarters)) &
        (sales_data['상권_구분_코드'] == target_business_code) &
        (sales_data['서비스_업종_코드'].isin(target_service_codes))
        ]
    #음식 관련 업종 매출 합계 구하기
    filtered_sales_data = filtered_sales_data.groupby(['기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명']).sum().reset_index()
    #MERGE
    merged_data = merge_data(filtered_resident_data,filtered_foot_trafic,filtered_sales_data,filtered_change_index)
    #중복 제거, 정렬
    merged_data = merged_data.drop_duplicates()
    merged_data = merged_data.sort_values(by=['기준_년분기_코드', '상권_코드'])

    return merged_data.to_csv(OUTPUT_PATH,index=False,encoding='utf-8-sig')

def merge_data(resident_data, floating_data, sales_data,change_index_data):
    merged_data = pd.merge(
        resident_data,
        floating_data,
        on=['기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명'],
        suffixes=('_상주', '_유동')
    )
    merged_data = pd.merge(
        merged_data,
        sales_data,
        on=['기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명'],
        suffixes=('', '_매출')
    )
    merged_data = pd.merge(
        merged_data,
        change_index_data,
        on=['기준_년분기_코드', '상권_구분_코드', '상권_코드', '상권_코드_명'],
        suffixes=('', '_변화지표'),
    )
    return merged_data
