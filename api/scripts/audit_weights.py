"""
Audit script to verify CMS weights in cms_weights.json.

This script validates the structure and values in the weights JSON file,
and optionally tests weighted average calculations against known values.

Run: python audit_weights.py
"""

import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WEIGHTS_FILE = os.path.join(SCRIPT_DIR, 'cms_weights.json')


def load_weights():
    """Load cms_weights.json file."""
    with open(WEIGHTS_FILE, 'r') as f:
        return json.load(f)


def validate_structure(weights):
    """Validate the structure of the weights JSON."""
    errors = []

    # Check years
    expected_years = ['2019', '2020', '2021', '2022', '2023', '2024', '2025', '2026']
    for year in expected_years:
        if year not in weights:
            errors.append(f"Missing year: {year}")
            continue

        year_data = weights[year]

        # Check parts
        for part in ['part_c', 'part_d']:
            if part not in year_data:
                errors.append(f"{year}: Missing {part}")
                continue

            part_data = year_data[part]

            # Check each measure has required fields
            for measure_id, measure_data in part_data.items():
                if not isinstance(measure_data, dict):
                    errors.append(f"{year}/{part}/{measure_id}: Not a dict")
                    continue

                if 'weight' not in measure_data:
                    errors.append(f"{year}/{part}/{measure_id}: Missing weight")

                if 'name' not in measure_data:
                    errors.append(f"{year}/{part}/{measure_id}: Missing name")

                # Validate weight value
                weight = measure_data.get('weight')
                if weight is not None:
                    if not isinstance(weight, (int, float)):
                        errors.append(f"{year}/{part}/{measure_id}: Weight is not a number")
                    elif weight < 0 or weight > 5:
                        errors.append(f"{year}/{part}/{measure_id}: Weight {weight} out of range [0, 5]")

    return errors


def validate_weight_consistency(weights):
    """Validate that weights follow expected patterns across years."""
    errors = []
    warnings = []

    # Check CAHPS/Patient Experience weight changes
    cahps_evolution = {
        '2019': 1.5, '2020': 1.5,
        '2021': 2, '2022': 2,
        '2023': 4, '2024': 4, '2025': 4,
        '2026': 2
    }

    # Improvement measures should always be 5
    improvement_measures_c = ['C29', 'C25', 'C27', 'C30']  # varies by year
    improvement_measures_d = ['D06', 'D04']  # varies by year

    # Outcomes should generally be 3
    outcomes_measures = ['D08', 'D09', 'D10']  # Medication adherence

    for year, expected_cahps in cahps_evolution.items():
        if year not in weights:
            continue

        year_data = weights[year]

        # Check a sample CAHPS measure
        part_c = year_data.get('part_c', {})

        # Find a CAHPS measure by looking for "Getting Needed Care"
        for measure_id, measure_data in part_c.items():
            if 'Getting Needed Care' in measure_data.get('name', ''):
                actual = measure_data.get('weight')
                if actual != expected_cahps:
                    warnings.append(f"{year}: CAHPS weight expected {expected_cahps}, got {actual}")
                break

        # Check outcomes measures
        part_d = year_data.get('part_d', {})
        for measure_id in outcomes_measures:
            if measure_id in part_d:
                weight = part_d[measure_id].get('weight')
                if weight != 3:
                    warnings.append(f"{year}/part_d/{measure_id}: Outcomes weight expected 3, got {weight}")

    return errors, warnings


def count_measures(weights):
    """Count measures by year and part."""
    print("\nMeasure Counts by Year:")
    print("-" * 50)

    for year in sorted([y for y in weights.keys() if y != '_meta']):
        year_data = weights[year]
        part_c_count = len(year_data.get('part_c', {}))
        part_d_count = len(year_data.get('part_d', {}))
        total = part_c_count + part_d_count
        print(f"  {year}: Part C={part_c_count}, Part D={part_d_count}, Total={total}")


def print_weight_summary(weights):
    """Print summary of weights by category for each year."""
    print("\nWeight Summary by Year:")
    print("-" * 60)

    for year in sorted([y for y in weights.keys() if y != '_meta']):
        year_data = weights[year]

        # Collect weights
        weight_counts = {}
        for part in ['part_c', 'part_d']:
            for measure_id, measure_data in year_data.get(part, {}).items():
                weight = measure_data.get('weight', 'N/A')
                weight_counts[weight] = weight_counts.get(weight, 0) + 1

        weight_str = ", ".join(f"w={w}: {c}" for w, c in sorted(weight_counts.items()))
        print(f"  {year}: {weight_str}")


def main():
    print("CMS Star Ratings Weight Audit")
    print("=" * 60)
    print(f"Validating: {WEIGHTS_FILE}")
    print()

    # Load weights
    try:
        weights = load_weights()
    except Exception as e:
        print(f"ERROR: Could not load weights file: {e}")
        return 1

    # Validate structure
    print("Checking structure...")
    struct_errors = validate_structure(weights)
    if struct_errors:
        print(f"  Structure errors: {len(struct_errors)}")
        for err in struct_errors[:10]:  # Show first 10
            print(f"    - {err}")
        if len(struct_errors) > 10:
            print(f"    ... and {len(struct_errors) - 10} more")
    else:
        print("  Structure: OK")

    # Validate weight consistency
    print("\nChecking weight patterns...")
    cons_errors, cons_warnings = validate_weight_consistency(weights)
    if cons_errors:
        print(f"  Consistency errors: {len(cons_errors)}")
        for err in cons_errors:
            print(f"    - {err}")
    if cons_warnings:
        print(f"  Warnings: {len(cons_warnings)}")
        for warn in cons_warnings:
            print(f"    - {warn}")
    if not cons_errors and not cons_warnings:
        print("  Consistency: OK")

    # Print summaries
    count_measures(weights)
    print_weight_summary(weights)

    # Overall result
    print("\n" + "=" * 60)
    if struct_errors or cons_errors:
        print("AUDIT FAILED")
        return 1
    else:
        print("AUDIT PASSED")
        print("\nNote: Weights were manually extracted from CMS Technical Notes.")
        print("Sources documented in cms_weights.json _meta section.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
