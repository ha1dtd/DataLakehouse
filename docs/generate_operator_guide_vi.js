const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  AlignmentType, HeadingLevel, BorderStyle, WidthType, ShadingType,
  LevelFormat, Header, Footer, PageNumber,
} = require("docx");
const fs = require("fs");
const path = require("path");

const cb = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: cb, bottom: cb, left: cb, right: cb };

function h1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    children: [new TextRun({ text, bold: true, size: 34, font: "Arial", color: "1F4E79" })],
    spacing: { before: 360, after: 120 },
  });
}

function h2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    children: [new TextRun({ text, bold: true, size: 26, font: "Arial", color: "2E75B6" })],
    spacing: { before: 220, after: 80 },
  });
}

function p(text) {
  return new Paragraph({
    children: [new TextRun({ text, size: 23, font: "Arial" })],
    spacing: { before: 40, after: 60 },
    alignment: AlignmentType.JUSTIFIED,
  });
}

function bullet(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "bullets", level },
    children: [new TextRun({ text, size: 23, font: "Arial" })],
    spacing: { before: 35, after: 35 },
  });
}

function step(title, detailLines) {
  return [
    new Paragraph({
      children: [new TextRun({ text: title, bold: true, size: 23, font: "Arial", color: "1A1A1A" })],
      spacing: { before: 80, after: 20 },
    }),
    ...detailLines.map((line) => bullet(line)),
  ];
}

function codeBlock(lines) {
  return new Table({
    width: { size: 9200, type: WidthType.DXA },
    columnWidths: [9200],
    rows: [new TableRow({
      children: [new TableCell({
        borders,
        shading: { fill: "F5F7FA", type: ShadingType.CLEAR },
        margins: { top: 90, bottom: 90, left: 140, right: 140 },
        children: lines.map((line) => new Paragraph({
          children: [new TextRun({ text: line, font: "Courier New", size: 20, color: "222222" })],
          spacing: { before: 10, after: 10 },
        })),
      })],
    })],
  });
}

function infoBox(label, lines, fill = "EBF5FB", borderColor = "2E75B6", labelColor = "1A5276", textColor = "1A3A4A") {
  return new Table({
    width: { size: 9200, type: WidthType.DXA },
    columnWidths: [9200],
    rows: [new TableRow({ children: [new TableCell({
      borders: {
        top: { style: BorderStyle.SINGLE, size: 4, color: borderColor },
        bottom: { style: BorderStyle.SINGLE, size: 4, color: borderColor },
        left: { style: BorderStyle.SINGLE, size: 14, color: borderColor },
        right: { style: BorderStyle.SINGLE, size: 4, color: borderColor },
      },
      shading: { fill, type: ShadingType.CLEAR },
      margins: { top: 100, bottom: 100, left: 180, right: 160 },
      children: [
        new Paragraph({
          children: [new TextRun({ text: label, bold: true, size: 22, font: "Arial", color: labelColor })],
          spacing: { before: 0, after: 40 },
        }),
        ...lines.map((line) => new Paragraph({
          children: [new TextRun({ text: line, size: 22, font: "Arial", color: textColor })],
          spacing: { before: 20, after: 20 },
          alignment: AlignmentType.JUSTIFIED,
        })),
      ],
    })] })],
  });
}

function makeTable(headers, rows, colWidths) {
  const headerRow = new TableRow({
    tableHeader: true,
    children: headers.map((h, i) => new TableCell({
      borders,
      width: { size: colWidths[i], type: WidthType.DXA },
      shading: { fill: "1F4E79", type: ShadingType.CLEAR },
      margins: { top: 90, bottom: 90, left: 120, right: 120 },
      children: [new Paragraph({
        children: [new TextRun({ text: h, bold: true, color: "FFFFFF", size: 22, font: "Arial" })],
      })],
    })),
  });
  const dataRows = rows.map((row, ri) => new TableRow({
    children: row.map((cell, ci) => new TableCell({
      borders,
      width: { size: colWidths[ci], type: WidthType.DXA },
      shading: { fill: ri % 2 === 0 ? "F7FBFF" : "FFFFFF", type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 120, right: 120 },
      children: [new Paragraph({
        children: [new TextRun({ text: cell, size: 21, font: "Arial" })],
      })],
    })),
  }));
  return new Table({
    width: { size: 9200, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [headerRow, ...dataRows],
  });
}

const doc = new Document({
  numbering: {
    config: [{
      reference: "bullets",
      levels: [{
        level: 0,
        format: LevelFormat.BULLET,
        text: "•",
        alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: 720, hanging: 360 } } },
      }],
    }],
  },
  sections: [{
    properties: {
      page: {
        size: { width: 11906, height: 16838 },
        margin: { top: 1200, right: 1200, bottom: 1200, left: 1200 },
      },
    },
    headers: {
      default: new Header({
        children: [new Paragraph({
          children: [new TextRun({ text: "FoxAI — Tài liệu hướng dẫn vận hành Data Platform", size: 18, font: "Arial", color: "888888" })],
          alignment: AlignmentType.RIGHT,
          border: { bottom: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC", space: 1 } },
        })],
      }),
    },
    footers: {
      default: new Footer({
        children: [new Paragraph({
          children: [
            new TextRun({ text: "Trang ", size: 18, font: "Arial", color: "888888" }),
            new TextRun({ children: [PageNumber.CURRENT], size: 18, font: "Arial", color: "888888" }),
          ],
          alignment: AlignmentType.CENTER,
          border: { top: { style: BorderStyle.SINGLE, size: 2, color: "CCCCCC", space: 1 } },
        })],
      }),
    },
    children: [
      new Paragraph({
        children: [new TextRun({ text: "Tài liệu hướng dẫn vận hành Data Platform", bold: true, size: 40, font: "Arial", color: "1F4E79" })],
        alignment: AlignmentType.CENTER,
        spacing: { before: 260, after: 80 },
      }),
      new Paragraph({
        children: [new TextRun({ text: "Phiên bản vận hành dành cho supervisor / operator", size: 24, font: "Arial", color: "555555" })],
        alignment: AlignmentType.CENTER,
        spacing: { before: 0, after: 80 },
      }),
      new Paragraph({
        children: [new TextRun({ text: "Hướng dẫn từng bước để vận hành hệ thống sau khi hoàn tất và bàn giao", size: 21, font: "Arial", color: "7A7A7A" })],
        alignment: AlignmentType.CENTER,
        spacing: { before: 0, after: 260 },
      }),

      h1("1. Thành phần cần vận hành"),
      makeTable(
        ["Thành phần", "Vai trò", "Địa chỉ / Ghi chú"],
        [
          ["Airflow", "Điều phối DAG production", "http://192.168.100.66:8081"],
          ["YARN", "Theo dõi Spark jobs", "http://192.168.100.66:8088"],
          ["MinIO", "Lưu trữ chính cho dữ liệu / lakehouse", "http://192.168.100.66:9001"],
          ["HDFS", "Hạ tầng runtime / compute tạm thời", "namenode internal:9000"],
          ["Spark on YARN", "Chạy job xử lý Bronze / Silver / Gold", "Không cần bật web riêng để trigger thường ngày"],
          ["Histogram Viewer", "Trang độc lập để xem histogram khi được bàn giao", "Không thuộc combined pipeline hiện tại"],
        ],
        [1800, 3600, 3800],
      ),

      h1("2. Quy trình khởi động trên server mới đã cài đặt xong"),
      p("Phần này bắt đầu từ thời điểm server đã được cài đặt và cấu hình xong. Mục tiêu là đưa toàn bộ nền tảng về trạng thái sẵn sàng vận hành."),

      h2("Bước 2.1 — Đăng nhập vào Namenode"),
      p("Toàn bộ lệnh vận hành chính đều chạy trên máy Namenode."),
      codeBlock([
        "ssh ubuntu@<IP-hoac-hostname-cua-namenode>",
      ]),

      h2("Bước 2.2 — Bật HDFS"),
      ...step("Thực hiện:", [
        "Chạy `start-dfs.sh` trên Namenode.",
        "Sau khi chạy xong, dùng `jps` để kiểm tra ít nhất có `NameNode` và `SecondaryNameNode` trên Namenode.",
      ]),
      codeBlock([
        "start-dfs.sh",
        "jps",
      ]),

      h2("Bước 2.3 — Bật YARN"),
      ...step("Thực hiện:", [
        "Chạy `start-yarn.sh` trên Namenode.",
        "Kiểm tra bằng `jps`; trên Namenode phải thấy `ResourceManager`.",
        "Nếu cần xem job Spark về sau, truy cập YARN UI tại cổng `8088`.",
      ]),
      codeBlock([
        "start-yarn.sh",
        "jps",
      ]),

      h2("Bước 2.4 — Kiểm tra MinIO"),
      ...step("Thực hiện:", [
        "Kiểm tra tiến trình MinIO bằng `pgrep -f \"minio server\"`.",
        "Nếu chưa chạy, bật lại MinIO bằng lệnh khởi động đã cấu hình cho server.",
        "MinIO là lớp lưu trữ chính; dữ liệu production phải đi vào các đường dẫn `s3a://...`, không lưu business data ở HDFS local path.",
      ]),
      codeBlock([
        "pgrep -f \"minio server\"",
        "nohup minio server ~/minio-data --address \":9001\" --console-address \":9002\" > ~/minio.log 2>&1 &",
      ]),

      h2("Bước 2.5 — Bật Airflow"),
      ...step("Thực hiện:", [
        "Kích hoạt môi trường Airflow trên Namenode.",
        "Bật scheduler ở chế độ nền.",
        "Bật webserver Airflow ở cổng `8081`.",
        "Sau khi bật, kiểm tra DAG production chính của hệ thống đã được Airflow nhận diện.",
      ]),
      codeBlock([
        "source ~/airflow-venv/bin/activate",
        "airflow scheduler -D",
        "airflow webserver -p 8081 -D",
        "airflow dags list | grep <ten_dag_production_duoc_ban_giao>",
      ]),

      h1("3. Truy cập giao diện vận hành"),
      h2("Bước 3.1 — Mở các giao diện chính"),
      ...step("Các trang cần dùng thường xuyên:", [
        "Airflow: `http://192.168.100.66:8081` — trigger và theo dõi DAG.",
        "YARN: `http://192.168.100.66:8088` — xem chi tiết job Spark khi cần debug.",
        "MinIO: `http://192.168.100.66:9001` — kiểm tra dữ liệu / bucket / artifact.",
      ]),
      infoBox("Thông tin đăng nhập thường dùng", [
        "Airflow: `admin / admin`",
        "MinIO: `admin / 12345678`",
        "Nếu môi trường production đổi mật khẩu, operator phải dùng giá trị đã bàn giao thực tế thay cho ví dụ ở trên.",
      ]),

      h1("4. Vận hành DAG production chính"),
      p("Ở phiên bản hoàn chỉnh, operator chỉ cần quan tâm tới luồng production chính của Data Platform: dữ liệu đi vào hệ thống, được xử lý qua các tầng chuẩn, và tạo ra đầu ra phục vụ khai thác. Tên DAG và chi tiết kỹ thuật có thể thay đổi theo gói bàn giao, nhưng quy trình vận hành tổng thể phải giữ nguyên."),
      makeTable(
        ["Mục", "Mô tả vận hành"],
        [
          ["Điểm vào dữ liệu", "Nguồn dữ liệu được hệ thống ingest theo cấu hình đã bàn giao."],
          ["Các tầng xử lý", "Dữ liệu đi qua raw / bronze / silver / gold theo đúng mô hình medallion."],
          ["Lưu trữ chính", "Dữ liệu và artifact nghiệp vụ nằm trên MinIO qua các đường dẫn `s3a://...`."],
          ["Điểm điều phối", "Airflow là nơi operator trigger run và theo dõi trạng thái toàn pipeline."],
          ["Điểm kiểm tra compute", "YARN dùng để xem job Spark khi cần kiểm tra sâu hơn."],
        ],
        [2600, 6600],
      ),

      h2("Bước 4.1 — Kiểm tra cấu hình nguồn dữ liệu trước khi chạy"),
      ...step("Thực hiện:", [
        "Xác nhận nguồn vào đã được khai báo đúng theo cấu hình bàn giao của hệ thống.",
        "Kiểm tra rằng bucket, đường dẫn dữ liệu, và các tham số môi trường đang trỏ đúng production.",
        "Nếu không có yêu cầu thay đổi chính thức, không chỉnh tay cấu hình ingest trước mỗi lần chạy.",
      ]),

      h2("Bước 4.2 — Trigger pipeline chính"),
      ...step("Trên Airflow UI:", [
        "Mở Airflow và tìm DAG production chính của hệ thống theo tên đã bàn giao.",
        "Đảm bảo DAG đang ở trạng thái bật (`On`) và không bị pause.",
        "Bấm `Trigger DAG` để chạy một phiên mới.",
      ]),

      h2("Bước 4.3 — Hiểu luồng xử lý dữ liệu"),
      p("Dù cách đặt tên task có thể khác nhau giữa các phiên bản bàn giao, pipeline hoàn chỉnh phải vận hành theo chuỗi xử lý chuẩn sau:"),
      codeBlock([
        "Nguồn vào",
        "-> Raw",
        "-> Bronze",
        "-> Silver",
        "-> Gold",
      ]),
      ...step("Cách theo dõi:", [
        "Ở bước đầu, hệ thống tiếp nhận dữ liệu đầu vào theo cấu hình ingest đã định nghĩa.",
        "Tầng raw lưu dữ liệu gốc để truy vết; tầng bronze chuẩn hóa định dạng; tầng silver làm sạch và chuẩn hóa nghiệp vụ; tầng gold tạo ra dữ liệu sẵn sàng khai thác.",
        "Nếu một task còn đang chạy trên Airflow, có thể mở YARN để xem Spark application tương ứng.",
        "Nếu một task bị lỗi, mở log Airflow của đúng task đó trước, sau đó mới xem log Spark trong YARN.",
      ]),

      h1("5. Kiểm tra đầu ra sau khi chạy"),
      h2("Bước 5.1 — Kiểm tra trạng thái tổng quát"),
      ...step("Một phiên chạy được xem là hoàn thành tốt khi:", [
        "Tất cả task của DAG production chính chuyển sang thành công.",
        "Không có job Spark treo lâu bất thường trên YARN.",
        "Dữ liệu ở các tầng raw / bronze / silver / gold xuất hiện đúng bucket trên MinIO.",
      ]),

      h2("Bước 5.2 — Kiểm tra dữ liệu trên MinIO"),
      p("Các warehouse chính hiện tại được cấu hình như sau:"),
      codeBlock([
        "s3a://raw/lakehouse/",
        "s3a://bronze/lakehouse/",
        "s3a://silver/lakehouse/",
        "s3a://gold/lakehouse/",
      ]),
      ...step("Thực hiện:", [
        "Đăng nhập MinIO UI.",
        "Mở đúng bucket tương ứng (`raw`, `bronze`, `silver`, `gold`).",
        "Kiểm tra thư mục `lakehouse/` và thời gian cập nhật object mới sau mỗi run.",
      ]),

      h2("Bước 5.3 — Kiểm tra bằng SQL khi cần"),
      p("Nếu Spark Thrift đã được bật riêng cho nhu cầu kiểm tra dữ liệu, operator có thể dùng SQL để xác nhận bảng và số dòng. Đây là bước tùy chọn, không bắt buộc cho mọi lần chạy."),
      codeBlock([
        "/opt/spark/bin/beeline -u 'jdbc:hive2://127.0.0.1:10000/default;auth=noSasl' -n ubuntu -e 'SHOW TABLES IN silver_catalog.default;'",
      ]),

      h1("6. Quy trình test an toàn cho operator"),
      ...step("Khi cần test mà không muốn làm nhiễu vận hành production:", [
        "Dùng đúng pipeline production, nhưng test bằng dữ liệu hoặc thay đổi đầu vào đã được kiểm soát trước.",
        "Không dùng các luồng thử nghiệm hoặc công cụ validation rời để thay thế quy trình production chính.",
        "Ghi lại `run_id` của Airflow khi test để truy vết log dễ hơn.",
        "Sau test, xác nhận output mới xuất hiện ở đúng tầng dữ liệu và không có task production khác bị ảnh hưởng.",
      ]),

      h1("7. Histogram — vai trò hiện tại"),
      p("Histogram là chức năng dùng để quan sát phân bố dữ liệu sau khi dữ liệu đã được xử lý đúng. Khi được tích hợp vào phiên bản hoàn chỉnh, operator cần hiểu rằng đầu vào của histogram phải là dữ liệu sạch, đúng schema, và đã đi qua pipeline chuẩn để kết quả biểu đồ có ý nghĩa."),
      ...step("Yêu cầu vận hành ở mức khái niệm:", [
        "Nguồn dữ liệu cấp cho histogram phải là dữ liệu đã được xử lý đúng và có chất lượng ổn định.",
        "Operator chỉ dùng histogram như công cụ quan sát và kiểm tra phân bố, không thay cho bước xác nhận thành công của pipeline chính.",
        "Ở thời điểm hiện tại, histogram vẫn đang là thành phần độc lập, chưa gộp vào luồng production cuối cùng; việc tích hợp sẽ được bổ sung ở giai đoạn hoàn thiện sản phẩm.",
      ]),

      h1("8. Vận hành sau khi hoàn tất Packaging + Plugin + Licensing"),
      p("Phần dưới đây mô tả mô hình vận hành mục tiêu sau khi 3 tính năng sản phẩm hoàn thành, để supervisor và operator chuẩn bị quy trình bàn giao ngay từ bây giờ."),
      makeTable(
        ["Tính năng", "Tác động tới operator"],
        [
          ["Packaging", "Core Python được đóng gói thành file nhị phân `.so`; operator không cần sửa source code production trên server."],
          ["Plugin", "Operator nhận plugin từ khách hàng / đội triển khai và đặt vào thư mục plugin chuẩn, thay vì chạm vào core code."],
          ["Licensing", "Operator phải quản lý file license key hợp lệ và kiểm tra giới hạn node / ngày hết hạn khi khởi động hệ thống."],
        ],
        [2200, 7000],
      ),

      h2("Bước 8.1 — Sau khi cài gói sản phẩm"),
      ...step("Mô hình vận hành mục tiêu:", [
        "Chạy `install.sh` một lần để cài đặt toàn bộ gói FoxAI lên Linux server.",
        "Đặt file license vào `/etc/foxai/license.key` theo gói bàn giao.",
        "Xác nhận hệ thống khởi động thành công sau khi license được xác thực.",
        "Tiếp tục vận hành DAG production và giao diện giám sát theo cùng quy trình operator ở các mục trên.",
      ]),

      h2("Bước 8.2 — Khi có plugin mới"),
      ...step("Thực hiện:", [
        "Nhận plugin theo chuẩn `BaseTransformer` từ đội triển khai / khách hàng.",
        "Đặt file plugin vào thư mục `/opt/foxai/plugins/`.",
        "Kiểm tra cấu hình plugin nếu gói plugin yêu cầu file config đi kèm.",
        "Trigger một run kiểm tra có kiểm soát trước khi dùng plugin trong luồng nghiệp vụ thật.",
      ]),

      h2("Bước 8.3 — Khi license có vấn đề"),
      ...step("Operator cần làm:", [
        "Kiểm tra file `/etc/foxai/license.key` còn đúng và chưa bị thay thế sai.",
        "Kiểm tra tình trạng hết hạn hoặc vượt quá giới hạn node được cấp phép.",
        "Nếu licensing server tạm thời mất kết nối, theo dõi grace period theo chính sách sản phẩm bàn giao.",
      ]),

      h1("9. Checklist nhanh mỗi lần vận hành"),
      ...step("Trước khi chạy:", [
        "HDFS đã chạy",
        "YARN đã chạy",
        "MinIO đang chạy",
        "Airflow scheduler + webserver đang chạy",
        "DAG production chính có trong Airflow",
      ]),
      ...step("Sau khi chạy:", [
        "Airflow run thành công",
        "YARN không còn job treo bất thường",
        "Dữ liệu mới xuất hiện ở đúng tầng raw / bronze / silver / gold",
        "Các artifact / bảng đầu ra được cập nhật đúng theo phạm vi run vừa thực hiện",
      ]),

      infoBox("Nguyên tắc vận hành quan trọng", [
        "Không dùng các DAG thử nghiệm để thay cho luồng production chính.",
        "Không lưu business data sang HDFS local path; dữ liệu chính phải ở MinIO / `s3a://`.",
        "Nếu cần điều tra lỗi dữ liệu, luôn bắt đầu từ Airflow task log -> YARN log -> MinIO output, theo đúng thứ tự.",
      ], "FCEEEE", "C0392B", "922B21", "4A1A14"),
    ],
  }],
});

const outDir = path.resolve(process.cwd(), "Docs");
fs.mkdirSync(outDir, { recursive: true });
const outFile = path.join(outDir, "Tai_lieu_huong_dan_van_hanh_Data_Platform.docx");

Packer.toBuffer(doc).then((buf) => {
  fs.writeFileSync(outFile, buf);
  console.log(outFile);
});
