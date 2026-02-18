import sys
from pathlib import Path


def run_test():
    """Run the address matcher test and output results for GitHub Actions"""
    try:
        # Make sure we have the required packages
        try:
            import pytest
        except ImportError as e:
            print(f"Missing required package: {e}")
            create_error_comment(f"Missing required package: {e}")
            return 1

        # Run the test using pytest and capture the result
        pytest_args = [
            "-xvs",
            "tests/test_edge_cases.py::test_address_matching_combined",
        ]
        result = pytest.main(pytest_args)

        # If the test failed completely, exit with error
        if result != 0 and result != pytest.ExitCode.TESTS_FAILED:
            print("Error running tests")
            create_error_comment("Error running tests. Check logs for details.")
            return 1

        # Get the test results from the pytest output capture
        test_results = getattr(pytest, "_test_results", None)

        if not test_results:
            print("No test results found")
            create_error_comment(
                "No test results found. The test may have failed to run properly."
            )
            return 1

        # Format the GitHub comment
        match_rate = f"Match rate: {test_results['match_rate']:.2f}%"
        total = test_results["total_cases"]
        correct = test_results["correct_matches"]
        total_reward = test_results["total_reward"]

        # Create GitHub Actions output
        with open(Path.cwd() / "github-comment.md", "w") as f:
            f.write("## 📊 Address Matcher Test Results\n\n")
            f.write("### Statistics\n\n")
            f.write(f"- **Total test cases:** {total}\n")
            f.write(f"- **Correct matches:** {correct}\n")
            f.write(f"- **{match_rate}**\n")
            f.write(f"- **Total reward:** {total_reward:.2f}\n\n")

            if test_results["mismatches"]:
                f.write("### ⚠️ Some addresses were not matched correctly:\n\n")

                for mismatch in test_results["mismatches"]:
                    f.write(f"#### Test Block ID: {mismatch['test_block_id']}\n\n")
                    f.write("| Record Type | Address | Postcode | Match Weight |\n")
                    f.write("| ----------- | ------- | -------- | ------------ |\n")

                    for record in mismatch["records"]:
                        weight_str = (
                            f"{record['match_weight']:.2f}"
                            if record["match_weight"] is not None
                            else "N/A"
                        )
                        f.write(
                            f"| {record['record_type']} | {record['address']} | {record['postcode']} | {weight_str} |\n"
                        )

                    f.write("\n")
            else:
                f.write("### ✅ All addresses were matched correctly!")

        print(f"Test completed: {match_rate}")
        return 0

    except Exception as e:
        print(f"Unexpected error: {e}")
        create_error_comment(f"Unexpected error: {e}")
        return 1


def create_error_comment(error_message):
    """Create a GitHub comment file with error information"""
    with open(Path.cwd() / "github-comment.md", "w") as f:
        f.write("## ❌ Address Matcher Test Failed\n\n")
        f.write(f"Error: {error_message}\n\n")
        f.write("Please check the workflow logs for more details.")


if __name__ == "__main__":
    sys.exit(run_test())
