#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <nlohmann/json.hpp> // Assuming you have included the JSON library

void convert_from_csv_to_hty(std::string csv_file_path, std::string hty_file_path) {
    std::ifstream csv_file(csv_file_path);
    std::ofstream hty_file(hty_file_path, std::ios::binary);

    if (!csv_file.is_open() || !hty_file.is_open()) {
        std::cerr << "Error opening file(s)." << std::endl;
        return;
    }

    std::string line;
    std::vector<std::vector<std::string>> data;
    while (std::getline(csv_file, line)) {
        std::stringstream ss(line);
        std::string value;
        std::vector<std::string> row;
        while (std::getline(ss, value, ',')) {
            row.push_back(value);
        }
        data.push_back(row);
    }

    // Create metadata
    nlohmann::json metadata;
    metadata["num_rows"] = data.size();
    metadata["num_groups"] = 1; // Assuming one group for simplicity
    metadata["groups"] = {
        {
            {"num_columns", data[0].size()},
            {"offset", 0}, // Assuming offset 0 for simplicity
            {"columns", nlohmann::json::array()}
        }
    };

    for (size_t i = 0; i < data[0].size(); ++i) {
        metadata["groups"][0]["columns"].push_back({
            {"column_name", "column" + std::to_string(i)},
            {"column_type", "int"} // Assuming all integers for simplicity
        });
    }

    // Write raw data
    for (const auto& row : data) {
        for (const auto& value : row) {
            int int_value = std::stoi(value);
            hty_file.write(reinterpret_cast<const char*>(&int_value), sizeof(int));
        }
    }

    // Write metadata
    std::string metadata_str = metadata.dump();
    hty_file.write(metadata_str.c_str(), metadata_str.size());

    // Write metadata size
    int metadata_size = metadata_str.size();
    hty_file.write(reinterpret_cast<const char*>(&metadata_size), sizeof(int));

    csv_file.close();
    hty_file.close();
}

int main() {
    std::string csv_file_path, hty_file_path;

    // Get arguments
    std::cout << "Please enter the .csv file path:" << std::endl;
    std::cin >> csv_file_path;
    std::cout << "Please enter the .hty file path:" << std::endl;
    std::cin >> hty_file_path;

    // Convert
    convert_from_csv_to_hty(csv_file_path, hty_file_path);

    return 0;
}