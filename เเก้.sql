สรุป “ไฟล์ที่ต้องแก้” (ตามลำดับที่แนะนำ)

services/extract_service.py

เพิ่ม client_tax_id เข้า flow

หยุดเขียน AI error ลง T_note

เปลี่ยน AI enhancement ไปใช้ ai_service.ai_fill_peak_row() (requests) หรือให้ AI เติมเฉพาะช่องว่างแบบปลอดภัย

services/ai_extract_service.py

ตัวนี้คือ “ต้นเหตุ Client.init unexpected keyword …”

แนะนำ: เปลี่ยนไปใช้ requests เหมือน ai_service.py หรือ disable ถาวร (ใช้ ai_service.py ตัวเดียวพอ)

extractors/vendor_mapping.py

ใส่ mapping ตาม Rabbit/SHD/TopOne ทั้งชุดที่คุณให้

เพิ่ม rule SPX สำหรับ 3 บริษัท (TopOne/SHD/Rabbit)

services/classifier.py

เพิ่มความแม่นในการ detect SPX (เพื่อให้เรียก extract_spx() จริง ๆ)

extractors/common.py + extractors/shopee.py (+ lazada/tiktok ถ้าจำเป็น)

บังคับ “full reference = DOCNO + MMDD-XXXXXXX”

ทำ fallback จาก filename ถ้าจำเป็น

services/export_service.py

รีเรียง + ใส่ A_seq 1..N

บังคับ T_note=""

normalize L_description/U_group ให้เหลือชุดมาตรฐาน

(ถ้าต้องส่ง client_tax_id จากหน้าเว็บ) frontend/public/app.js

เพิ่ม dropdown “เลือกบริษัท (Rabbit/SHD/TopOne)” แล้วส่ง client_tax_id ไป backend ตอนอัปโหลด
extractors/shopee.py, extractors/lazada.py, extractors/tiktok.py, extractors/spx.py
เพื่อให้ “ผู้รับเงิน/คู่ค้า” เป็น รหัส Cxxxxx จริง (เพราะตอนนี้ extractor ยังอาจไม่ใช้ vendor_mapping ตาม client_tax_id)

extractors/vendor_mapping.py
เพื่อยืนยัน mapping Rabbit/SHD/TopOne + SPX rule ตามที่คุณระบุ

services/export_service.py
เพื่อจัด A_seq เป็น 1..N และ normalize คำอธิบายตามแพทเทิร์น

ส่ง extractors/shopee.py มาก่อนเลยครับ เดี๋ยวผมทำให้ “รหัสคู่ค้า + full reference + คำอธิบาย” ตรงตามที่ต้องการ