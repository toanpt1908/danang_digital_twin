import pandas as pd
import re
from transformers import pipeline
from underthesea import word_tokenize

def load_advanced_sentiment_model():
    """
    Khởi tạo mô hình AI phân tích cảm xúc tiếng Việt (PhoBERT).
    """
    print("Đang tải bộ não NLP PhoBERT...")
    # Sử dụng mô hình PhoBERT chuyên dụng cho phân loại cảm xúc
    analyzer = pipeline("sentiment-analysis", model="wonrax/phobert-base-vietnamese-sentiment")
    return analyzer

def clean_and_segment_text(raw_text):
    """
    Làm sạch dữ liệu và sử dụng underthesea để tách từ chuẩn tiếng Việt.
    Đây là bước quyết định độ chính xác của mô hình!
    """
    if not isinstance(raw_text, str):
        return ""
        
    # 1. Làm sạch cơ bản: Xóa HTML và ký tự đặc biệt
    text = re.sub(r'<.*?>', ' ', raw_text)
    text = re.sub(r'[^\w\s]', ' ', text)
    text = " ".join(text.split())
    
    # 2. Tách từ bằng underthesea
    # Tham số format="text" sẽ nối các từ ghép bằng dấu gạch dưới (VD: "Đà_Nẵng")
    segmented_text = word_tokenize(text, format="text")
    
    return segmented_text

def analyze_sentiment_high_accuracy(text, analyzer):
    """
    Tiến hành phân tích và trả về (Điểm số, Nhãn cảm xúc)
    """
    # Xử lý văn bản bằng underthesea trước khi đưa vào mô hình
    processed_text = clean_and_segment_text(text)
    
    # Giới hạn độ dài đầu vào để mô hình không bị quá tải (khoảng 256 token)
    short_text = processed_text[:600] 
    
    if not short_text:
        return 0, 'Trung lập'

    try:
        # Đưa văn bản đã tách từ vào mô hình dự đoán
        result = analyzer(short_text)[0]
        model_label = result['label']
        
        # Chuyển đổi nhãn về định dạng Star Schema
        if model_label == 'POS':
            return 1, 'Tích cực'
        elif model_label == 'NEG':
            return -1, 'Tiêu cực'
        else:
            return 0, 'Trung lập'
            
    except Exception as e:
        print(f"Lỗi khi xử lý đoạn văn: {e}")
        return 0, 'Trung lập'

def process_news_pipeline(input_file, output_file):
    """
    Hàm thực thi chính để đọc, xử lý và lưu dữ liệu
    """
    print(f"Đang đọc tập dữ liệu: {input_file}")
    df = pd.read_csv(input_file)
    
    model = load_advanced_sentiment_model()
    
    print("Bắt đầu xử lý tách từ và phân tích cảm xúc (Quá trình này có thể mất chút thời gian)...")
    
    # Tạo 2 cột mới trong DataFrame dựa trên kết quả phân tích
    df[['sentiment_score', 'sentiment_label']] = df['content'].apply(
        lambda x: pd.Series(analyze_sentiment_high_accuracy(x, model))
    )
    
    df.to_csv(output_file, index=False)
    print(f"Hoàn thành xuất sắc! Dữ liệu lớp Silver đã lưu tại: {output_file}")

def analyze_sentiment_batch(texts, analyzer, batch_size=16):
    """
    Xử lý theo lô (Batch Processing) cho hàng ngàn bài báo.
    Tự động áp dụng chuẩn tách từ underthesea và logic quy đổi điểm 1, -1, 0.
    """
    # 1. Làm sạch và tách từ (Segment) cho toàn bộ danh sách
    # Giữ lại tối đa 600 ký tự đầu tiên tương tự logic của bạn
    processed_texts = [clean_and_segment_text(text)[:600] for text in texts]
    
    # Đảm bảo không có chuỗi rỗng gây lỗi AI
    processed_texts = [t if t.strip() else "trống" for t in processed_texts]

    # 2. Đưa toàn bộ vào AI xử lý một lần
    try:
        results = analyzer(processed_texts, batch_size=batch_size, truncation=True)
    except Exception as e:
        print(f"Lỗi khi chạy lô AI: {e}")
        # Trả về mặc định nếu lỗi toàn bộ lô
        return [0] * len(texts), ['Trung lập'] * len(texts)

    # 3. Chuyển đổi nhãn (Map labels) khớp 100% với kiến trúc cũ
    scores = []
    labels = []
    for res in results:
        model_label = res['label']
        if model_label == 'POS':
            scores.append(1)
            labels.append('Tích cực')
        elif model_label == 'NEG':
            scores.append(-1)
            labels.append('Tiêu cực')
        else:
            scores.append(0)
            labels.append('Trung lập')

    return scores, labels

# ==========================================
# KHU VỰC KIỂM THỬ MÃ (TESTING)
# ==========================================
if __name__ == "__main__":
    # Khởi tạo mô hình
    ai_model = load_advanced_sentiment_model()
    
    # Câu văn mẫu để kiểm tra
    cau_test = "Hạ tầng du lịch thành phố dạo này xuống cấp nghiêm trọng, nhân viên phục vụ rất kém."
    
    # Xem thử cách underthesea xử lý chuỗi
    chuoi_da_tach = clean_and_segment_text(cau_test)
    print(f"\n[1] Chuỗi sau khi underthesea tách từ:\n -> {chuoi_da_tach}")
    
    # Xem kết quả cuối cùng
    diem, nhan = analyze_sentiment_high_accuracy(cau_test, ai_model)
    print(f"\n[2] Đánh giá cuối cùng:\n -> Điểm: {diem} | Nhãn: {nhan}")