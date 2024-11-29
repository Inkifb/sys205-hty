#include "analyze.hpp"

nlohmann::json extract_metadata(std::string hty_file_path) {
    std::string metadata_file = hty_file_path + ".json";
    std::ifstream f(metadata_file);
    if (!f) {
        throw std::runtime_error("Cannot open metadata file: " + metadata_file);
    }
    nlohmann::json metadata = nlohmann::json::parse(f);
    return metadata;
}

std::vector<int> project_single_column(nlohmann::json metadata,
                                     std::string hty_file_path,
                                     std::string projected_column) {
    // Get column index from metadata
    int column_index = -1;
    auto columns = metadata["columns"];
    for (size_t i = 0; i < columns.size(); i++) {
        if (columns[i]["name"] == projected_column) {
            column_index = i;
            break;
        }
    }
    
    if (column_index == -1) {
        throw std::runtime_error("Column not found: " + projected_column);
    }

    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file) {
        throw std::runtime_error("Cannot open HTY file: " + hty_file_path);
    }

    std::vector<int> column_data;
    int num_columns = metadata["columns"].size();
    int value;

    hty_file.seekg(column_index * sizeof(int));
    while (hty_file.read(reinterpret_cast<char*>(&value), sizeof(int))) {
        column_data.push_back(value);
        
        // Skip to next row's position for our column
        if (!hty_file.seekg((num_columns - 1) * sizeof(int), std::ios::cur)) {
            break;
        }
    }

    hty_file.close();
    return column_data;
}

void display_column(nlohmann::json metadata, std::string column_name,
                   std::vector<int> data) {
    // Verify column exists in metadata
    bool column_found = false;
    for (const auto& column : metadata["columns"]) {
        if (column["name"] == column_name) {
            column_found = true;
            break;
        }
    }
    
    if (!column_found) {
        throw std::runtime_error("Column not found in metadata: " + column_name);
    }

    std::cout << "Column: " << column_name << std::endl;
    std::cout << "----------------" << std::endl;
    for (size_t i = 0; i < data.size(); i++) {
        std::cout << "Row " << (i + 1) << ": " << data[i] << std::endl;
    }

    std::cout << "----------------" << std::endl;
    std::cout << "Total rows: " << data.size() << std::endl;
}

std::vector<int> filter(nlohmann::json metadata, std::string hty_file_path,
                       std::string projected_column, int operation,
                       int filtered_value) {
    std::vector<int> column_data = project_single_column(metadata, hty_file_path, 
                                                       projected_column);
    std::vector<int> filtered_data;

    for (const int& value : column_data) {
        bool include = false;
        switch (operation) {
            case 0: 
                include = (value == filtered_value);
                break;
            case 1: 
                include = (value != filtered_value);
                break;
            case 2: 
                include = (value < filtered_value);
                break;
            case 3: 
                include = (value > filtered_value);
                break;
            case 4: 
                include = (value <= filtered_value);
                break;
            case 5: 
                include = (value >= filtered_value);
                break;
            default:
                throw std::runtime_error("Invalid operation code: " + 
                                       std::to_string(operation));
        }

        if (include) {
            filtered_data.push_back(value);
        }
    }

    return filtered_data;
}

std::vector<std::vector<int>> project(
    nlohmann::json metadata, std::string hty_file_path,
    std::vector<std::string> projected_columns) {
    
    // Get column indices from metadata
    std::vector<int> column_indices;
    auto columns = metadata["columns"];
    
    for (const auto& proj_col : projected_columns) {
        bool found = false;
        for (size_t i = 0; i < columns.size(); i++) {
            if (columns[i]["name"] == proj_col) {
                column_indices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::runtime_error("Column not found: " + proj_col);
        }
    }

    // Open HTY file in binary mode
    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file) {
        throw std::runtime_error("Cannot open HTY file: " + hty_file_path);
    }

    // Initialize result set
    std::vector<std::vector<int>> result_set(projected_columns.size());
    int total_columns = metadata["columns"].size();
    int value;

    while (true) {
        bool row_complete = true;
        for (int col = 0; col < total_columns; col++) {
            if (!hty_file.read(reinterpret_cast<char*>(&value), sizeof(int))) {
                row_complete = false;
                break;
            }
            
            // Check if this column should be included in result
            auto it = std::find(column_indices.begin(), column_indices.end(), col);
            if (it != column_indices.end()) {
                int result_col = it - column_indices.begin();
                result_set[result_col].push_back(value);
            }
        }
        if (!row_complete) break;
    }

    hty_file.close();
    return result_set;
}

void display_result_set(nlohmann::json metadata,
                       std::vector<std::string> column_names,
                       std::vector<std::vector<int>> result_set) {
    // Verify all columns exist in metadata
    for (const auto& col_name : column_names) {
        bool found = false;
        for (const auto& column : metadata["columns"]) {
            if (column["name"] == col_name) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::runtime_error("Column not found in metadata: " + col_name);
        }
    }

    if (result_set.size() != column_names.size()) {
        throw std::runtime_error("Result set size doesn't match column count");
    }

    // Verify all columns have the same number of rows
    if (!result_set.empty()) {
        size_t row_count = result_set[0].size();
        for (const auto& column : result_set) {
            if (column.size() != row_count) {
                throw std::runtime_error("Inconsistent row counts in result set");
            }
        }
    }

    std::cout << "Result Set:" << std::endl;
    std::cout << std::string(20 * column_names.size(), '-') << std::endl;
    
    for (const auto& name : column_names) {
        std::cout << std::left << std::setw(20) << name;
    }
    std::cout << std::endl;
    std::cout << std::string(20 * column_names.size(), '-') << std::endl;

    // Display data rows
    if (!result_set.empty()) {
        for (size_t row = 0; row < result_set[0].size(); row++) {
            for (size_t col = 0; col < result_set.size(); col++) {
                std::cout << std::left << std::setw(20) << result_set[col][row];
            }
            std::cout << std::endl;
        }
    }

    std::cout << std::string(20 * column_names.size(), '-') << std::endl;
    std::cout << "Total rows: " << (result_set.empty() ? 0 : result_set[0].size()) 
              << std::endl;
}

std::vector<std::vector<int>> project_and_filter(
    nlohmann::json metadata, std::string hty_file_path,
    std::vector<std::string> projected_columns, std::string filtered_column,
    int op, int value) {
    
    // First get the filtered indices by applying filter on the filtered_column
    std::vector<int> filtered_data = filter(metadata, hty_file_path, 
                                          filtered_column, op, value);
    
    // Get the full data for all requested columns
    std::vector<std::vector<int>> all_data = project(metadata, hty_file_path, 
                                                    projected_columns);
    
    // Find the index of filtered_column in projected_columns
    int filter_col_idx = -1;
    for (size_t i = 0; i < projected_columns.size(); i++) {
        if (projected_columns[i] == filtered_column) {
            filter_col_idx = i;
            break;
        }
    }

    // If filtered column wasn't in projected columns, get its data separately
    std::vector<int> filter_col_data;
    if (filter_col_idx == -1) {
        filter_col_data = project_single_column(metadata, hty_file_path, 
                                              filtered_column);
    }

    // Create result set with filtered rows
    std::vector<std::vector<int>> result_set(projected_columns.size());
    
    // For each row in the original data
    size_t total_rows = all_data[0].size();
    for (size_t row = 0; row < total_rows; row++) {
        // Check if this row should be included
        int check_value;
        if (filter_col_idx != -1) {
            check_value = all_data[filter_col_idx][row];
        } else {
            check_value = filter_col_data[row];
        }

        bool include = false;
        switch (op) {
            case 0: include = (check_value == value); break;
            case 1: include = (check_value != value); break;
            case 2: include = (check_value < value); break;
            case 3: include = (check_value > value); break;
            case 4: include = (check_value <= value); break;
            case 5: include = (check_value >= value); break;
            default:
                throw std::runtime_error("Invalid operation code: " + 
                                       std::to_string(op));
        }

        // If row matches filter, add all column values to result
        if (include) {
            for (size_t col = 0; col < projected_columns.size(); col++) {
                result_set[col].push_back(all_data[col][row]);
            }
        }
    }

    return result_set;
}

void add_row(nlohmann::json metadata, std::string hty_file_path,
             std::string modified_hty_file_path,
             std::vector<std::vector<int>> rows) {
    // Validate number of columns in input matches metadata
    int expected_columns = metadata["columns"].size();
    for (const auto& row : rows) {
        if (row.size() != static_cast<size_t>(expected_columns)) {
            throw std::runtime_error("Invalid row size. Expected " + 
                                   std::to_string(expected_columns) + 
                                   " columns, got " + 
                                   std::to_string(row.size()));
        }
    }

    // Open source HTY file for reading
    std::ifstream source_file(hty_file_path, std::ios::binary);
    if (!source_file) {
        throw std::runtime_error("Cannot open source HTY file: " + hty_file_path);
    }

    // Open destination HTY file for writing
    std::ofstream dest_file(modified_hty_file_path, std::ios::binary);
    if (!dest_file) {
        throw std::runtime_error("Cannot create destination HTY file: " + 
                               modified_hty_file_path);
    }

    dest_file << source_file.rdbuf();
    source_file.close();

    // Append new rows
    for (const auto& row : rows) {
        for (const auto& value : row) {
            dest_file.write(reinterpret_cast<const char*>(&value), sizeof(int));
        }
    }

    dest_file.close();

    std::ifstream verify_file(modified_hty_file_path, std::ios::binary);
    if (!verify_file) {
        throw std::runtime_error("Failed to verify written file: " + 
                               modified_hty_file_path);
    }
    verify_file.close();
}
