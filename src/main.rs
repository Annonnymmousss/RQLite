use anyhow::{Result, bail};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            let table_count = read_number_of_tables(&mut file)?;

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            println!("database page size: {}", page_size);
            println!("number of tables: {}", table_count);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let table_names = read_table_names(&mut file)?;
            if !table_names.is_empty() {
                println!("{}", table_names.join(" "));
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
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
    let (payload_size, len1) = read_varint(page, cell_offset);
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
