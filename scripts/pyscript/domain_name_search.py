"""Main execution function."""
import os
import mypy_utils as mypy

# 
def main():
    """Main execution function."""
    try:
        parent_directory="C:\\Users\\rmedi\\OneDrive\\Documents\\Files\\"
        domain_log=f"{parent_directory}domain_log.csv"
        domain_words_path=f"{parent_directory}domain_search_words.csv"
        count_checked = 0
        count_skipped = 0
        root_words = mypy.get_column_as_set(domain_words_path, 0)
        first_only_words = mypy.get_column_as_set(domain_words_path, 1)
        second_only_words = mypy.get_column_as_set(domain_words_path, 2)
        # an array of tuples, each tuple is a pattern configuration
        pattern_configs = [
            (first_only_words, second_only_words),
            (first_only_words, root_words),
            (root_words, second_only_words),
            (root_words, root_words),
            (root_words, root_words, second_only_words),
            (first_only_words, root_words, root_words),
            (first_only_words, root_words, second_only_words)
        ]

        # Load history_set of Checked Domains
        history_set = mypy.get_column_as_set(domain_log, 0)

        print(f"Loaded {len(history_set)} existing domains from history_set. Skipping these.")

        print("Starting scan for NEW combinations...")

        #unique_domains = set()
        unique_domains = mypy.generate_patterns(pattern_configs)
        tab_list = []
        for domain in unique_domains:
            # history_set CHECK: Skip if already in CSV
            if domain in history_set:
                count_skipped += 1
                continue
            if not mypy.is_domain_registered(domain):
                # Add to history_set set in case of duplicates within this same run
                tab_list.append([domain, "Available"])
            else:
                tab_list.append([domain, ""])
            count_checked += 1
            mypy.progress_bar(count_checked, len(unique_domains), description="Checking Domains")
        print("\n--------Appending Domains to File----------")
        mypy.append_to_csv(domain_log, tab_list)
        print("Run Complete.")
        print(f"New Domains Checked: {count_checked}")
        print(f"Skipped (Already in CSV): {count_skipped}")
        print("------------------------------------------------")
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        os._exit(1)

if __name__ == "__main__":
    main()
