use anyhow::{bail, Result};
use std::fs::File;
use std::io::{prelude::*, Seek, SeekFrom};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let db_path = &args[1];
    let command = &args[2];

    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(db_path)?;
            let mut header = [0u8; 100];
            file.read_exact(&mut header)?;

            let page_size = u16::from_be_bytes([header[16], header[17]]);
            let table_count = read_number_of_tables(&mut file)?;

            eprintln!("Logs from your program will appear here!");

            println!("database page size: {}", page_size);
            println!("number of tables: {}", table_count);
        }
        ".tables" => {
            let mut file = File::open(db_path)?;
            let table_names = read_table_names(&mut file)?;
            if !table_names.is_empty() {
                println!("{}", table_names.join(" "));
            }
        }
        _ => {
            let mut file = File::open(db_path)?;
            let upper = command.to_uppercase();

            if upper.starts_with("SELECT") && upper.contains("COUNT(*)") {
                let table_name = parse_table_name(command);
                let count = count_rows_in_table(&mut file, &table_name)?;
                println!("{}", count);
            } else if upper.starts_with("SELECT") {
                if upper.contains("WHERE") {
                    let (cols, table, where_col, where_val) =
                        parse_select_columns_where_query(command);
                    let rows =
                        select_columns_from_table_where(&mut file, &table, &cols, &where_col, &where_val)?;
                    for row in rows {
                        println!("{}", row.join("|"));
                    }
                } else {
                    let (cols, table) = parse_select_columns_query(command);
                    if cols.len() == 1 {
                        let values = select_column_from_table(&mut file, &table, &cols[0])?;
                        for v in values {
                            println!("{}", v);
                        }
                    } else {
                        let rows = select_columns_from_table(&mut file, &table, &cols)?;
                        for row in rows {
                            println!("{}", row.join("|"));
                        }
                    }
                }
            } else {
                bail!("Missing or invalid command passed: {}", command)
            }
        }
    }

    Ok(())
}

fn read_number_of_tables(file: &mut File) -> Result<u16> {
    let mut page_header = [0u8; 8];
    file.read_exact(&mut page_header)?;
    let cell_count = u16::from_be_bytes([page_header[3], page_header[4]]);
    Ok(cell_count)
}

fn read_table_names(file: &mut File) -> Result<Vec<String>> {
    let mut header = [0u8; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

    let mut page = vec![0u8; page_size];
    page[..100].copy_from_slice(&header);
    file.read_exact(&mut page[100..])?;

    let page_header_offset = 100;
    let cell_count =
        u16::from_be_bytes([page[page_header_offset + 3], page[page_header_offset + 4]]) as usize;
    let cell_ptr_array_offset = page_header_offset + 8;

    let mut names = Vec::new();
    for i in 0..cell_count {
        let idx = cell_ptr_array_offset + i * 2;
        let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
        let name = extract_tbl_name_from_cell(&page, cell_offset)?;
        names.push(name);
    }

    Ok(names)
}

fn extract_tbl_name_from_cell(page: &[u8], cell_offset: usize) -> Result<String> {
    let (_payload_size, len1) = read_varint(page, cell_offset);
    let (_rowid, len2) = read_varint(page, cell_offset + len1);
    let header_start = cell_offset + len1 + len2;
    let (header_size, len3) = read_varint(page, header_start);
    let mut header_pos = header_start + len3;

    let mut serials = [0u64; 5];
    for i in 0..5 {
        let (st, l) = read_varint(page, header_pos);
        serials[i] = st;
        header_pos += l;
    }

    let body_start = header_start + header_size as usize;
    let mut body_pos = body_start;

    for col in 0..5 {
        let size = serial_type_size(serials[col]);
        if col == 2 {
            let start = body_pos;
            let end = start + size;
            let bytes = &page[start..end];
            let s = String::from_utf8(bytes.to_vec())?;
            return Ok(s);
        }
        body_pos += size;
    }

    Ok(String::new())
}

fn read_varint(buf: &[u8], offset: usize) -> (u64, usize) {
    let mut result: u64 = 0;
    let mut i = 0;

    loop {
        let b = buf[offset + i];
        if i == 8 {
            result = (result << 8) | b as u64;
            i += 1;
            break;
        } else {
            result = (result << 7) | (b & 0x7F) as u64;
            if (b & 0x80) == 0 {
                i += 1;
                break;
            }
        }
        i += 1;
    }

    (result, i)
}

fn serial_type_size(serial: u64) -> usize {
    match serial {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 => 8,
        7 => 8,
        8 => 0,
        9 => 0,
        10 | 11 => 0,
        s if s >= 12 && s % 2 == 0 => ((s - 12) / 2) as usize,
        s if s >= 13 && s % 2 == 1 => ((s - 13) / 2) as usize,
        _ => 0,
    }
}

struct SchemaRow {
    tbl_name: String,
    rootpage: u32,
    sql: String,
}

fn parse_table_name(query: &str) -> String {
    let parts: Vec<&str> = query.split_whitespace().collect();
    let last = parts.last().unwrap_or(&"");
    last.trim_end_matches(';').to_string()
}

fn count_rows_in_table(file: &mut File, table_name: &str) -> Result<usize> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

    let mut page = vec![0u8; page_size];
    page[..100].copy_from_slice(&header);
    file.read_exact(&mut page[100..])?;

    let page_header_offset = 100;
    let cell_count =
        u16::from_be_bytes([page[page_header_offset + 3], page[page_header_offset + 4]]) as usize;
    let cell_ptr_array_offset = page_header_offset + 8;

    let mut rootpage: Option<u32> = None;

    for i in 0..cell_count {
        let idx = cell_ptr_array_offset + i * 2;
        let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
        let row = extract_schema_row_from_cell(&page, cell_offset)?;
        if row.tbl_name == table_name {
            rootpage = Some(row.rootpage);
            break;
        }
    }

    let rootpage = match rootpage {
        Some(r) => r,
        None => bail!("table not found"),
    };

    let mut count = 0usize;
    scan_table_btree_count(file, rootpage, page_size, &mut count)?;
    Ok(count)
}

fn scan_table_btree_count(
    file: &mut File,
    page_no: u32,
    page_size: usize,
    count: &mut usize,
) -> Result<()> {
    let page_start: u64 = (page_no as u64 - 1) * page_size as u64;
    file.seek(SeekFrom::Start(page_start))?;
    let mut page = vec![0u8; page_size];
    file.read_exact(&mut page)?;

    let header_offset = if page_no == 1 { 100 } else { 0 };
    let page_type = page[header_offset];
    let cell_count =
        u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;

    if page_type == 0x0D {
        *count += cell_count;
    } else if page_type == 0x05 {
        let right_child = u32::from_be_bytes([
            page[header_offset + 8],
            page[header_offset + 9],
            page[header_offset + 10],
            page[header_offset + 11],
        ]);
        let cell_ptr_array_offset = header_offset + 12;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page[cell_offset],
                page[cell_offset + 1],
                page[cell_offset + 2],
                page[cell_offset + 3],
            ]);
            scan_table_btree_count(file, child_page, page_size, count)?;
        }
        scan_table_btree_count(file, right_child, page_size, count)?;
    }

    Ok(())
}

fn extract_schema_row_from_cell(page: &[u8], cell_offset: usize) -> Result<SchemaRow> {
    let (_payload_size, len1) = read_varint(page, cell_offset);
    let (_rowid, len2) = read_varint(page, cell_offset + len1);
    let header_start = cell_offset + len1 + len2;
    let (header_size, len3) = read_varint(page, header_start);
    let mut header_pos = header_start + len3;

    let mut serials = [0u64; 5];
    for i in 0..5 {
        let (st, l) = read_varint(page, header_pos);
        serials[i] = st;
        header_pos += l;
    }

    let body_start = header_start + header_size as usize;
    let mut body_pos = body_start;

    let mut tbl_name = String::new();
    let mut rootpage: u32 = 0;
    let mut sql = String::new();

    for col in 0..5 {
        let size = serial_type_size(serials[col]);
        if col == 2 {
            let bytes = &page[body_pos..body_pos + size];
            tbl_name = String::from_utf8(bytes.to_vec())?;
        } else if col == 3 {
            let bytes = &page[body_pos..body_pos + size];
            let mut v: u64 = 0;
            for b in bytes {
                v = (v << 8) | (*b as u64);
            }
            rootpage = v as u32;
        } else if col == 4 {
            let bytes = &page[body_pos..body_pos + size];
            sql = String::from_utf8(bytes.to_vec())?;
        }
        body_pos += size;
    }

    Ok(SchemaRow { tbl_name, rootpage, sql })
}

fn parse_select_columns_query(query: &str) -> (Vec<String>, String) {
    let upper = query.to_uppercase();
    let select_pos = upper.find("SELECT").unwrap_or(0);
    let from_pos = upper.find("FROM").unwrap_or(query.len());
    let cols_part = &query[select_pos + 6..from_pos];
    let cols: Vec<String> = cols_part
        .split(',')
        .map(|c| c.trim().trim_end_matches(',').to_string())
        .filter(|c| !c.is_empty())
        .collect();

    let after_from = &query[from_pos + 4..];
    let table = after_from
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(';')
        .to_string();

    (cols, table)
}

fn parse_select_columns_where_query(query: &str) -> (Vec<String>, String, String, String) {
    let upper = query.to_uppercase();
    let select_pos = upper.find("SELECT").unwrap_or(0);
    let from_pos = upper.find("FROM").unwrap_or(query.len());
    let where_pos = upper.find("WHERE").unwrap_or(query.len());

    let cols_part = &query[select_pos + 6..from_pos];
    let cols: Vec<String> = cols_part
        .split(',')
        .map(|c| c.trim().trim_end_matches(',').to_string())
        .filter(|c| !c.is_empty())
        .collect();

    let table_part = if where_pos < query.len() {
        &query[from_pos + 4..where_pos]
    } else {
        &query[from_pos + 4..]
    };
    let table = table_part
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(';')
        .to_string();

    let where_part = if where_pos < query.len() {
        &query[where_pos + 5..]
    } else {
        ""
    };

    let mut where_col = String::new();
    let mut where_val = String::new();

    if !where_part.trim().is_empty() {
        let eq_parts: Vec<&str> = where_part.splitn(2, '=').collect();
        if eq_parts.len() == 2 {
            where_col = eq_parts[0].trim().to_string();
            let mut v = eq_parts[1].trim().trim_end_matches(';').trim().to_string();
            if v.starts_with('\'') && v.ends_with('\'') && v.len() >= 2 {
                v = v[1..v.len() - 1].to_string();
            } else if v.starts_with('"') && v.ends_with('"') && v.len() >= 2 {
                v = v[1..v.len() - 1].to_string();
            }
            where_val = v;
        }
    }

    (cols, table, where_col, where_val)
}

fn select_column_from_table(file: &mut File, table_name: &str, column_name: &str) -> Result<Vec<String>> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

    let mut page = vec![0u8; page_size];
    page[..100].copy_from_slice(&header);
    file.read_exact(&mut page[100..])?;

    let page_header_offset = 100;
    let cell_count =
        u16::from_be_bytes([page[page_header_offset + 3], page[page_header_offset + 4]]) as usize;
    let cell_ptr_array_offset = page_header_offset + 8;

    let mut target_row: Option<SchemaRow> = None;
    for i in 0..cell_count {
        let idx = cell_ptr_array_offset + i * 2;
        let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
        let row = extract_schema_row_from_cell(&page, cell_offset)?;
        if row.tbl_name == table_name {
            target_row = Some(row);
            break;
        }
    }

    let schema_row = match target_row {
        Some(r) => r,
        None => bail!("table not found"),
    };

    let col_index = get_column_index_from_sql(&schema_row.sql, column_name)?;

    let rows = scan_table_btree_column(file, schema_row.rootpage, page_size, col_index)?;
    Ok(rows)
}

fn scan_table_btree_column(
    file: &mut File,
    page_no: u32,
    page_size: usize,
    col_index: usize,
) -> Result<Vec<String>> {
    let page_start: u64 = (page_no as u64 - 1) * page_size as u64;
    file.seek(SeekFrom::Start(page_start))?;
    let mut page = vec![0u8; page_size];
    file.read_exact(&mut page)?;

    let header_offset = if page_no == 1 { 100 } else { 0 };
    let page_type = page[header_offset];
    let cell_count =
        u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;

    let mut result = Vec::new();

    if page_type == 0x0D {
        let cell_ptr_array_offset = header_offset + 8;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            if let Some(val) = extract_column_from_table_cell(&page, cell_offset, col_index)? {
                result.push(val);
            }
        }
    } else if page_type == 0x05 {
        let right_child = u32::from_be_bytes([
            page[header_offset + 8],
            page[header_offset + 9],
            page[header_offset + 10],
            page[header_offset + 11],
        ]);
        let cell_ptr_array_offset = header_offset + 12;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page[cell_offset],
                page[cell_offset + 1],
                page[cell_offset + 2],
                page[cell_offset + 3],
            ]);
            let mut child_vals =
                scan_table_btree_column(file, child_page, page_size, col_index)?;
            result.append(&mut child_vals);
        }
        let mut right_vals = scan_table_btree_column(file, right_child, page_size, col_index)?;
        result.append(&mut right_vals);
    }

    Ok(result)
}

fn select_columns_from_table(
    file: &mut File,
    table_name: &str,
    columns: &[String],
) -> Result<Vec<Vec<String>>> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

    let mut page = vec![0u8; page_size];
    page[..100].copy_from_slice(&header);
    file.read_exact(&mut page[100..])?;

    let page_header_offset = 100;
    let cell_count =
        u16::from_be_bytes([page[page_header_offset + 3], page[page_header_offset + 4]]) as usize;
    let cell_ptr_array_offset = page_header_offset + 8;

    let mut target_row: Option<SchemaRow> = None;
    for i in 0..cell_count {
        let idx = cell_ptr_array_offset + i * 2;
        let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
        let row = extract_schema_row_from_cell(&page, cell_offset)?;
        if row.tbl_name == table_name {
            target_row = Some(row);
            break;
        }
    }

    let schema_row = match target_row {
        Some(r) => r,
        None => bail!("table not found"),
    };

    let mut indexes = Vec::new();
    for col in columns {
        let idx = get_column_index_from_sql(&schema_row.sql, col)?;
        indexes.push(idx);
    }

    let rows = scan_table_btree_all_columns(file, schema_row.rootpage, page_size, &indexes)?;
    Ok(rows)
}

fn select_columns_from_table_where(
    file: &mut File,
    table_name: &str,
    columns: &[String],
    where_col: &str,
    where_val: &str,
) -> Result<Vec<Vec<String>>> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0u8; 100];
    file.read_exact(&mut header)?;
    let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

    let mut page = vec![0u8; page_size];
    page[..100].copy_from_slice(&header);
    file.read_exact(&mut page[100..])?;

    let page_header_offset = 100;
    let cell_count =
        u16::from_be_bytes([page[page_header_offset + 3], page[page_header_offset + 4]]) as usize;
    let cell_ptr_array_offset = page_header_offset + 8;

    let mut table_row: Option<SchemaRow> = None;
    let mut index_row: Option<SchemaRow> = None;
    let where_lower = where_col.to_lowercase();

    for i in 0..cell_count {
        let idx = cell_ptr_array_offset + i * 2;
        let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
        let row = extract_schema_row_from_cell(&page, cell_offset)?;
        if row.tbl_name == table_name {
            let sql_lower = row.sql.to_lowercase();
            if sql_lower.contains("create table") && table_row.is_none() {
                table_row = Some(row);
            } else if sql_lower.contains("create index")
                && sql_lower.contains(&where_lower)
                && index_row.is_none()
            {
                index_row = Some(row);
            }
        }
    }

    let table_schema = match table_row {
        Some(r) => r,
        None => bail!("table not found"),
    };

    let mut indexes = Vec::new();
    for col in columns {
        let idx = get_column_index_from_sql(&table_schema.sql, col)?;
        indexes.push(idx);
    }
    let where_index = get_column_index_from_sql(&table_schema.sql, where_col)?;

    if let Some(index_schema) = index_row {
        let rowids =
            scan_index_btree_for_value(file, index_schema.rootpage, page_size, where_val)?;
        let mut rows = Vec::new();
        for rid in rowids {
            if let Some(row_vals) =
                scan_table_btree_for_rowid(file, table_schema.rootpage, page_size, rid, &indexes)?
            {
                rows.push(row_vals);
            }
        }
        Ok(rows)
    } else {
        let rows = scan_table_btree_where(
            file,
            table_schema.rootpage,
            page_size,
            &indexes,
            where_index,
            where_val,
        )?;
        Ok(rows)
    }
}

fn scan_table_btree_where(
    file: &mut File,
    page_no: u32,
    page_size: usize,
    indexes: &[usize],
    where_index: usize,
    where_val: &str,
) -> Result<Vec<Vec<String>>> {
    let page_start: u64 = (page_no as u64 - 1) * page_size as u64;
    file.seek(SeekFrom::Start(page_start))?;
    let mut page = vec![0u8; page_size];
    file.read_exact(&mut page)?;

    let header_offset = if page_no == 1 { 100 } else { 0 };
    let page_type = page[header_offset];
    let cell_count =
        u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;

    let mut rows = Vec::new();

    if page_type == 0x0D {
        let cell_ptr_array_offset = header_offset + 8;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;

            let where_v = extract_column_from_table_cell(&page, cell_offset, where_index)?;
            if let Some(ref w) = where_v {
                if w == where_val {
                    let mut row_vals = Vec::new();
                    for &col_idx in indexes {
                        let v = extract_column_from_table_cell(&page, cell_offset, col_idx)?;
                        row_vals.push(v.unwrap_or_default());
                    }
                    rows.push(row_vals);
                }
            }
        }
    } else if page_type == 0x05 {
        let right_child = u32::from_be_bytes([
            page[header_offset + 8],
            page[header_offset + 9],
            page[header_offset + 10],
            page[header_offset + 11],
        ]);
        let cell_ptr_array_offset = header_offset + 12;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page[cell_offset],
                page[cell_offset + 1],
                page[cell_offset + 2],
                page[cell_offset + 3],
            ]);
            let mut child_rows = scan_table_btree_where(
                file,
                child_page,
                page_size,
                indexes,
                where_index,
                where_val,
            )?;
            rows.append(&mut child_rows);
        }
        let mut right_rows =
            scan_table_btree_where(file, right_child, page_size, indexes, where_index, where_val)?;
        rows.append(&mut right_rows);
    }

    Ok(rows)
}

fn scan_table_btree_all_columns(
    file: &mut File,
    page_no: u32,
    page_size: usize,
    indexes: &[usize],
) -> Result<Vec<Vec<String>>> {
    let page_start: u64 = (page_no as u64 - 1) * page_size as u64;
    file.seek(SeekFrom::Start(page_start))?;
    let mut page = vec![0u8; page_size];
    file.read_exact(&mut page)?;

    let header_offset = if page_no == 1 { 100 } else { 0 };
    let page_type = page[header_offset];
    let cell_count =
        u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;

    let mut rows = Vec::new();

    if page_type == 0x0D {
        let cell_ptr_array_offset = header_offset + 8;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;

            let mut row_vals = Vec::new();
            for &col_idx in indexes {
                let v = extract_column_from_table_cell(&page, cell_offset, col_idx)?;
                row_vals.push(v.unwrap_or_default());
            }
            rows.push(row_vals);
        }
    } else if page_type == 0x05 {
        let right_child = u32::from_be_bytes([
            page[header_offset + 8],
            page[header_offset + 9],
            page[header_offset + 10],
            page[header_offset + 11],
        ]);
        let cell_ptr_array_offset = header_offset + 12;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page[cell_offset],
                page[cell_offset + 1],
                page[cell_offset + 2],
                page[cell_offset + 3],
            ]);
            let mut child_rows =
                scan_table_btree_all_columns(file, child_page, page_size, indexes)?;
            rows.append(&mut child_rows);
        }
        let mut right_rows =
            scan_table_btree_all_columns(file, right_child, page_size, indexes)?;
        rows.append(&mut right_rows);
    }

    Ok(rows)
}

fn scan_table_btree_for_rowid(
    file: &mut File,
    page_no: u32,
    page_size: usize,
    target_rowid: u64,
    indexes: &[usize],
) -> Result<Option<Vec<String>>> {
    let page_start: u64 = (page_no as u64 - 1) * page_size as u64;
    file.seek(SeekFrom::Start(page_start))?;
    let mut page = vec![0u8; page_size];
    file.read_exact(&mut page)?;

    let header_offset = if page_no == 1 { 100 } else { 0 };
    let page_type = page[header_offset];
    let cell_count =
        u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;

    if page_type == 0x0D {
        let cell_ptr_array_offset = header_offset + 8;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;

            let (_payload_size, len1) = read_varint(&page, cell_offset);
            let (rowid, _len2) = read_varint(&page, cell_offset + len1);
            if rowid == target_rowid {
                let mut row_vals = Vec::new();
                for &col_idx in indexes {
                    let v = extract_column_from_table_cell(&page, cell_offset, col_idx)?;
                    row_vals.push(v.unwrap_or_default());
                }
                return Ok(Some(row_vals));
            }
        }
        Ok(None)
    } else if page_type == 0x05 {
        let right_child = u32::from_be_bytes([
            page[header_offset + 8],
            page[header_offset + 9],
            page[header_offset + 10],
            page[header_offset + 11],
        ]);
        let cell_ptr_array_offset = header_offset + 12;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page[cell_offset],
                page[cell_offset + 1],
                page[cell_offset + 2],
                page[cell_offset + 3],
            ]);
            if let Some(vals) =
                scan_table_btree_for_rowid(file, child_page, page_size, target_rowid, indexes)?
            {
                return Ok(Some(vals));
            }
        }
        if let Some(vals) =
            scan_table_btree_for_rowid(file, right_child, page_size, target_rowid, indexes)?
        {
            return Ok(Some(vals));
        }
        Ok(None)
    } else {
        Ok(None)
    }
}

fn scan_index_btree_for_value(
    file: &mut File,
    page_no: u32,
    page_size: usize,
    target_val: &str,
) -> Result<Vec<u64>> {
    let page_start: u64 = (page_no as u64 - 1) * page_size as u64;
    file.seek(SeekFrom::Start(page_start))?;
    let mut page = vec![0u8; page_size];
    file.read_exact(&mut page)?;

    let header_offset = if page_no == 1 { 100 } else { 0 };
    let page_type = page[header_offset];
    let cell_count =
        u16::from_be_bytes([page[header_offset + 3], page[header_offset + 4]]) as usize;

    let mut rowids = Vec::new();

    if page_type == 0x0A {
        let cell_ptr_array_offset = header_offset + 8;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let (val, rid) = extract_index_key_and_rowid_from_cell(&page, cell_offset)?;
            if val == target_val {
                rowids.push(rid);
            }
        }
    } else if page_type == 0x02 {
        let right_child = u32::from_be_bytes([
            page[header_offset + 8],
            page[header_offset + 9],
            page[header_offset + 10],
            page[header_offset + 11],
        ]);
        let cell_ptr_array_offset = header_offset + 12;
        for i in 0..cell_count {
            let idx = cell_ptr_array_offset + i * 2;
            let cell_offset = u16::from_be_bytes([page[idx], page[idx + 1]]) as usize;
            let child_page = u32::from_be_bytes([
                page[cell_offset],
                page[cell_offset + 1],
                page[cell_offset + 2],
                page[cell_offset + 3],
            ]);
            let mut child_rowids =
                scan_index_btree_for_value(file, child_page, page_size, target_val)?;
            rowids.append(&mut child_rowids);
        }
        let mut right_rowids =
            scan_index_btree_for_value(file, right_child, page_size, target_val)?;
        rowids.append(&mut right_rowids);
    }

    Ok(rowids)
}

fn extract_index_key_and_rowid_from_cell(
    page: &[u8],
    cell_offset: usize,
) -> Result<(String, u64)> {
    let (_payload_size, len1) = read_varint(page, cell_offset);
    let record_start = cell_offset + len1;

    let (header_size, len2) = read_varint(page, record_start);
    let mut header_pos = record_start + len2;

    let mut serials = Vec::new();
    while header_pos < record_start + header_size as usize {
        let (st, l) = read_varint(page, header_pos);
        serials.push(st);
        header_pos += l;
    }

    let body_start = record_start + header_size as usize;
    let mut body_pos = body_start;

    let mut key = String::new();
    let mut rowid: u64 = 0;

    for (idx, st) in serials.iter().enumerate() {
        let size = serial_type_size(*st);
        if idx == 0 {
            let bytes = &page[body_pos..body_pos + size];
            key = String::from_utf8(bytes.to_vec())?;
        } else if idx == serials.len() - 1 {
            let bytes = &page[body_pos..body_pos + size];
            let mut v: u64 = 0;
            for b in bytes {
                v = (v << 8) | (*b as u64);
            }
            rowid = v;
        }
        body_pos += size;
    }

    Ok((key, rowid))
}

fn get_column_index_from_sql(sql: &str, column_name: &str) -> Result<usize> {
    let lower_sql = sql.to_lowercase();
    let start = match lower_sql.find('(') {
        Some(i) => i + 1,
        None => bail!("invalid sql"),
    };
    let end = match lower_sql.rfind(')') {
        Some(i) => i,
        None => bail!("invalid sql"),
    };
    let cols_str = &sql[start..end];
    let parts: Vec<&str> = cols_str.split(',').collect();
    for (i, part) in parts.iter().enumerate() {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut iter = trimmed.split_whitespace();
        if let Some(name) = iter.next() {
            let clean = name.trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']');
            if clean.eq_ignore_ascii_case(column_name) {
                return Ok(i);
            }
        }
    }
    bail!("column not found")
}

fn extract_column_from_table_cell(
    page: &[u8],
    cell_offset: usize,
    col_index: usize,
) -> Result<Option<String>> {
    let (_payload_size, len1) = read_varint(page, cell_offset);
    let (rowid, len2) = read_varint(page, cell_offset + len1);
    let header_start = cell_offset + len1 + len2;
    let (header_size, len3) = read_varint(page, header_start);
    let mut header_pos = header_start + len3;

    let mut serials = Vec::new();
    while header_pos < header_start + header_size as usize {
        let (st, l) = read_varint(page, header_pos);
        serials.push(st);
        header_pos += l;
    }

    if col_index >= serials.len() {
        return Ok(None);
    }

    let body_start = header_start + header_size as usize;
    let mut body_pos = body_start;

    for (idx, st) in serials.iter().enumerate() {
        let size = serial_type_size(*st);
        if idx == col_index {
            if size == 0 {
                if col_index == 0 {
                    return Ok(Some(rowid.to_string()));
                } else {
                    return Ok(None);
                }
            }
            let bytes = &page[body_pos..body_pos + size];
            let s = String::from_utf8(bytes.to_vec())?;
            return Ok(Some(s));
        }
        body_pos += size;
    }

    Ok(None)
}
