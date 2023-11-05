[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/odCL_uKO)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12422407&assignment_repo_type=AssignmentRepo)
# Graded Challenge 7 - Set 1

_Graded Challenge 7 ini dibuat guna mengevaluasi pembelajaran pada Hacktiv8 Data Science Fulltime Program khususnya pada Phase 2 Week 1._

---

## Assignment Objectives

Graded Challenge 7 ini dibuat guna mengevaluasi konsep NoSQL pada pembelajaran Phase 2 sebagai berikut:

- Mampu memahami konsep NoSQL secara keseluruhan.

- Mampu mempersiapkan data untuk digunakan sebelum masuk ke database NoSQL.

- Mampu mengolah dan memvisualisasikan data dengan menggunakan Kibana.

---

## Dataset

### Ketentuan Dataset
1. Pilihlah dataset yang paling nyaman digunakan dalam mengerjakan Graded Challenge 7. Adapun ketentuan dataset yang harus digunakan adalah :
   * Setidaknya terdapat minimal 10 column.
   * Setiap column terdiri dari :
     + Capital letter dan lower letter, atau
     + Semua huruf merupakan huruf kapital
     + Contoh : `Age`, `fullName`, `CITY`
   * Tidak diperbolehkan memilih dataset dimana nama column terdiri dari lowercase saja.

2. **Konsultasikan terlebih dahulu dataset yang hendak digunakan ke buddy masing-masing student. Jika disetujui, maka silakan dikerjakan. Jika tidak disetujui, maka cari dataset yang lain dan konsultasikan lagi mengenai dataset yang baru ini.**

3. Student tidak boleh menggunakan dataset yang sudah dipakai dalam sesi pembelajaran saat dikelas bersama instruktur atau dataset pada tugas-tugas terdahulu dari Phase 0 hingga Phase 2.

4. **Student dilarang untuk melakukan scraping dataset** karena dikhawatirkan proses pembuatan scraper dan proses scraping akan memakan waktu. Gunakan public dataset yang tersedia diberbagai macam situs Internet.

### Data Sources
Student dapat memilih dataset dari salah satu repository dibawah ini. Popular open data repositories :

- [UC Irvine Machine Learning Repository](https://archive.ics.uci.edu/ml/index.php)
- [Kaggle datasets](https://www.kaggle.com/datasets)
- [Amazon’s AWS datasets](https://registry.opendata.aws/)

Meta portals :

- [Data Portals](http://dataportals.org/)
- [OpenDataMonitor](https://opendatamonitor.eu/frontend/web/index.php?r=dashboard%2Findex)
- [Quandl](https://www.quandl.com/)
- Sumber lain yang kredibel.

---

## Problems

Objective : Buatlah report yang berisi Exploratory Data Analysis dengan menggunakan dataset yang sudah terlebih dahulu dilakukan Data Cleaning. 

Berikut ini adalah langkah-langkah yang harus dilakukan : 

1. Tentukan dataset yang hendak dipakai. Beri nama dataset ini dengan `P2G7_<nama-student>_data_raw.csv`. Contoh : `P2G7_raka_ardhi_data_raw.csv`.
  
2. Masukan data tersebut ke dalam PostgreSQL local masing-masing student. Beri nama table untuk menyimpan data tersebut dengan `table_gc7`.

3. Setelah data berada didalam database, ambil semua data dari database dengan menggunakan Python dan lakukan beberapa Data Cleaning berikut ini dengan menggunakan Python :
   - Hapus data yang duplikat.
   - Fiksasi tipe masing-masing column/attribute.
     + Contoh 1 : misalkan terdapat column `Child` saat pengambilan data berbentuk floating point (`2.0`, `1.0`) diubah menjadi tipe integer (`2`, `1`).
     + Contoh 2 : column `salary` saat pengambilan data berbentuk string yang seharusnya integer.
   - Normalisasi column dengan cara : 
     + Semua nama column menjadi lowercase. Contoh : `ID` → `id`, `EDUCATION` → `education`, `Age` → `age`.
     + Spasi pada nama column diubah menjadi `_` (underscore). Contoh : `First Name` → `first_name`, `HOME ADDRESS` → `home_address`.
     + Menghapus spasi/tab/simbol yang dirasa tidak diperlukan pada nama column. Contoh : `  name` → `name`, `|car_price|` → `car_price`.
   - Handling Missing Values

4. Setelah dilakukan Data Cleaning, simpan data clean ini ke dalam CSV file dengan nama `P2G7_<nama-student>_data_clean.csv`. Contoh : `P2G7_raka_ardhi_data_clean.csv`

5. Selain disimpan ke dalam file CSV, data clean ini juga akan dimasukkan ke dalam Elasticsearch dengan menggunakan Python.

6. Buatlah dashboard dengan Kibana terhadap data clean ini dengan ketentuan :
   - Jelaskan mengenai objective Exploratory Data Analysis yang hendak dilakukan. Nyatakan secara jelas mengenai report yang dibuat seperti :
     * Latar belakang adanya report tersebut
     * Tujuan yang hendak dicapai
     * Divisi/tim yang membutuhkan
     * dll
   - Buatlah minimal 6 visualisasi terhadap data tersebut yang mendukung tercapainya objective dari proses EDA yang dilakukan. Adapun 6 visualisasi harus menggunakan plot seperti :
     * 1 penggunaan Bar Plot
     * 1 penggunaan Pie Chart
     * 1 penggunaan Vertical Bar Plot
     * 3 plot lainnya dibebaskan mengenai jenisnya namun tidak boleh menggunakan jenis plot yang sama dengan plot yang sudah ada.
   - Setelah sebuah plot terbentuk, berikan narasi/insight yang dapat diambil dari plot tersebut. Anda bisa meletakkan insight ini dibawah plot atau disamping plot.
   - Tambahkan 1 visualisasi dibagian atas berupa `Markdown` yang berisi :
     + Identitas student.
     + Penjelasan objective.
   - Tambahkan 1 visualisasi dibagian akhir berupa `Markdown` yang berisi :
     + Kesimpulan eksplorasi yang dilakukan.
     + Saran lanjutan atau insight bisnis terhadap eksplorasi yang dilakukan.
     + Kesimpulan akan lebih bagus jika ditambahkan data/statement dari suatu sumber eksternal yang sejalan dengan kesimpulan yang didapatkan.
   - Total visualisasi : 6 visualisasi + 1 visualisasi Markdown mengenai indetitas + 1 visualisasi Markdown mengenai kesimpulan = 8 visualiasi.
   - Student dipersilakan membuat skenario/situasi fiksi terhadap dataset yang dipakai.
   - Student dipersilakan untuk mengaplikasikan teori mengenai Business Knowledge pada tugas ini.

7. Screenshot setiap plot dan insight.
   * Buat sebuah folder bernama `images`.
   * Masukkan semua screenshot ke dalam folder tersebut.
   * Sebuah plot dan insightnya akan dimasukkan ke dalam screenshot yang sama. 
   * Screenshot juga bagian mengenai Identitas, Objective, dan Kesimpulan.

---
## Conceptual Problems

*Jawab pertanyaan berikut:*

1. Jelaskan apa yang dimaksud dengan NoSQL menggunakan pemahaman yang kalian ketahui !

2. Jelaskan kapan harus menggunakan NoSQL dan Relational Database Management System !

3. Sebutkan contoh 2 tools/platform NoSQL selain ElasticSearch beserta keunggulan tools/platform tersebut !

---

## Assignment Instructions

*Graded Challenge 7* dikerjakan dalam format ***Python script (.py)*** dengen beberapa **kriteria wajib** di bawah ini:

1. *Project* dinyatakan selesai dan diterima untuk dinilai jika script dapat dijalankan dengan baik di prompt maupun terminal.

2. Pada tugas Graded Challenge 7, student akan diminta untuk membuat :
   1. `P2G7_<nama-student>_ddl.sql`
      - File ini berisi syntax DDL untuk pembuatan database dan table.
      - File ini berisi syntax DML untuk melakukan insert data ke database. Anda bisa menggunakan perintah `COPY` untuk melakukan insert data.
      - Contoh penamaan : `P2G7_raka_ardhi_ddl.sql`
   2. `P2G7_<nama-student>_data_raw.csv`
      - File ini berisi dataset original yang akan dimasukkan ke dalam database PostgreSQL.
      - Contoh penamaan : `P2G7_raka_ardhi_data_raw.csv`.
   3. `P2G7_<nama-student>_data_clean.csv`
      - File ini berisi data yang telah dilakukan Data Cleaning.
      - Contoh penamaan : `P2G7_raka_ardhi_data_clean.csv`.
   4. `P2G7_<nama-student>.py`
      - File ini berisi :
        + Python code untuk mengambil data dari database PostgreSQL.
        + Python code untuk melakukan proses Data Cleaning seperti yang sudah ditentukan.
        + Python code untuk menyimpan data clean ke file CSV.
        + Python code untuk memasukkan data clean ke dalam Elastic Search.
      - Contoh penamaan : `P2G7_raka_ardhi.py`.
   5. `P2G7_<nama-student>_conceptual.txt`.
      - File ini berisi jawaban conceptual problem.
   6. `/images`.
      - Folder ini berisi daftar screenshot.
      - Contoh penamaan :
        * `introduction & objective.png`.
        * `plot & insight 01.png`.
        * `plot & insight 02.png`.
        * `plot & insight 03.png`.
        * `plot & insight 04.png`.
        * `plot & insight 05.png`.
        * `plot & insight 06.png`.
        * `kesimpulan.png`.

3. **Tidak diperkenankan** membuat program dalam format notebook.

4. Pada file Python, **wajib** memberikan keterangan atau pengenalan dengan menggunakan `comment` atau `docstring` yang berisikan : Judul tugas, Nama, Batch, dan penjelasan singkat tentang program yang dibuat, fitur-fitur. Contoh:
    ```py
    '''
    =================================================
    Graded Challenge 7

    Nama  : Raka Ardhi
    Batch : FTDS-001-RMT

    Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
    =================================================
    '''
    ```

5. Anda diwajibkan menggunakan class/function untuk memisahkan bagian code agar flow dari code yang dibuat mudah diikuti. Berikan penjelasan pada setiap class/function yang dibuat dengan menggunakan docstring seperti : 
   ```py
   def get_data_from_postgresql(url, database, table):
     '''
     Fungsi ini ditujukan untuk mengambil data dari PostgreSQL untuk selanjutnya dilakukan Data Cleaning.

     Parameters:
      url: string - lokasi PostgreSQL
      database: string - nama database dimana data disimpan
      table: string - nama table dimana data disimpan

     Return
      data: list of str - daftar data yang ada di database
        
     Contoh penggunaan:
     data = get_data_from_postgresql('localhost', 'db_phase2', 'table_gc7')
     '''

     return data

   ```

---

## Assignment Submission

- Push Assignment yang telah Anda buat ke akun GitHub Classroom Anda masing-masing.
- Contoh bentuk repository :
  ```
  P2-GC7-Set-1/raka-ardhi
  |
  ├── P2G7_raka_ardhi_ddl.sql
  ├── P2G7_raka_ardhi_data_raw.csv
  ├── P2G7_raka_ardhi_data_clean.csv
  ├── P2G7_raka_ardhi.py
  ├── P2G7_raka_ardhi_conceptual.txt
  ├── README.md
  ├── /images
        ├── introduction & objective.png
        ├── plot & insight 01.png
        ├── plot & insight 02.png
        ├── plot & insight 03.png
        ├── plot & insight 04.png
        ├── plot & insight 05.png
        ├── plot & insight 06.png
        └── kesimpulan.png
  ```

---

## Assignment Rubrics

### Code Review

| Criteria | Meet Expectations | Points |
| --- | --- | --- |
| Data Visualization | Mampu membuat minimal 6 visualisasi dengan menggunakan Kibana | 4 pts / visualisasi |
| Insight | Menampilkan **insight di setiap visualisasi** yang ditampilkan pada dashboard | 4 pts / insight |
| Conclusion | Penarikan kesimpulan yang dilakukan sejalan dengan tujuan dilakukannya eksplorasi dan terdapat saran/tindakan lanjutan/rekomendasi terhadap insight yang dihasilkan | 8 pts |
| Runs Perfectly | Kode berjalan tanpa ada error. Seluruh kode berfungsi dan dibuat dengan benar. | 10 pts |

### Concepts

| Criteria | Meet Expectations | Points |
| --- | --- | --- |
| NoSQL | Mampu menjawab pertanyaan dengan singkat, jelas, dan padat serta sesuai dengan konsep dan logika yang ada mengenai Conceptual Problems (3 pts each) | 9 pts |

### Readability

| Criteria | Meet Expectations | Points |
| --- | --- | --- |
| Tertata Dengan Baik | Semua baris kode terdokumentasi dengan baik dengan Markdown untuk penjelasan kode | 15 pts |

```
Kriteria tertata dengan baik diantaranya adalah: 

1. Tidak menyalin markdown dari tugas lain.
2. Import library rapih (terdapat dalam 1 cell dan tidak ada unused libs).
3. Terdapat komentar pada setiap baris kode.
4. Adanya pemisah yang jelas antar section, dll.
5. Tidak adanya typo.
```

---

```
Total Points : 90
```

---

## Notes

* **Deadline : P2W2D1 pukul 23:59 WIB.**

* **Keterlambatan pengumpulan tugas mengakibatkan skor Graded Challenge 7 menjadi 0.**
