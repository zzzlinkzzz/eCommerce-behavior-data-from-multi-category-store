# eCommerce-behavior-data-from-multi-category-store

### Nguồn dữ liệu:
Dữ liệu là các tương tác (click) của khách hàng trên một cửa hàng thương mại điện tử trong tháng 10 và tháng 11 năm 2019.
* Tổng số tương tác trong tháng 11/2019: 67.5 triệu tương tác.
* Các trường dữ liệu:
  - event_time: thời điểm xảy ra tương tác
  - event_type: Phân loại tương tác (view / cart / purchase)
  - product_id: 190.662 mã sản phẩm
  - category_id: 684 mã nhóm hàng
  - category_code: 129 tên nhóm hàng
  - brand: 4.201 tên hãng
  - price: giá sản phẩm
  - user_id: 3.696.117 mã khách hàng
  - user_session: 13.776.050 phiên truy cập, thay đổi khi khách hàng truy cập lại sau 1 khoảng thời gian dài.

src: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

### Vấn đề:
1. Theo từng phiên giao dịch, dựa vào giỏ hàng của khách hàng, dự báo khách hàng có mua hàng hay không để có khuyến mãi phù hợp.
2. Phân tích thị hiếu của khách hàng dựa trên nhóm hàng hóa thường xuyên.

### Hướng giải quyết:
* Do số lượng tương tác lớn (67.5 triệu click - ~10GB) do đó cần sử dụng công cụ khai phá dữ liệu phân tán và song song => lựa chọn Spark
* Dữ liệu dạng bảng => có thể sử dụng SQL trong môi trường Spark => tăng hiệu quả phân tích, giảm thời gian viết code, tận dụng được tốc độ của Spark
* Dự báo khách hàng có mua hàng hay không => khai thác đặc trưng dữ liệu theo phiên truy cập, có thể sử dụng các mô hình phân loại nông.
* Phân tích thị hiếu của khách hàng => khai thác đặc trưng dữ liệu theo mã khách hàng, sử dụng luật kết hợp (association rule)

### Công cụ/ngôn ngữ:
* Python, SQL
* PySpark
* Pandas
* Scikit-learn

### Vấn đề 1: dự báo hành vi mua hàng theo phiên truy cập:

1. Khai thác đặc trưng dữ liệu (feature engineering):

* Tổng số loại tương tác theo từng phiên:

| user_session	| cart	| purchase	| view	|
| -------------- | --- | --- | --- | 
| 000506dd-c8f9-46e2-895c-70a78cde8e17	| 1	| 0	| 14	|
| 001be2de-46a6-429d-b568-fd52d8af3f7d	| 1	| 1	| 1	|

* Tìm thời điểm bắt đầu, kết thúc và khoảng thời gian xảy ra phiên truy cập:

| user_session	| event_time_first	| event_time_last	| duration	|
| -------------- | --- | --- | --- | 
| eddf5cd0-c926-4920-a5b4-b2a65b7779b7	| 2019-11-17 01:29:09	| 2019-11-17 01:43:58	889	|
| 5c1e2dcb-f090-4014-b5f2-77db180404e5	| 2019-11-17 01:33:04	| 2019-11-17 02:21:00	2876	|
