#!/usr/bin/env python3
"""
Test script for running MCPMark Filesystem benchmark with vLLM model.
This script includes full logging to see model inputs/outputs.
"""
import asyncio
import logging
import sys
from datetime import datetime

# Configure logging to show everything
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'log/mcpmark/filesystem_vllm_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

from mcpuniverse.tracer.collectors import FileCollector
from mcpuniverse.benchmark.runner import BenchmarkRunner
from mcpuniverse.benchmark.report import BenchmarkReport
from mcpuniverse.callbacks.handlers.vprint import get_vprint_callbacks


async def main():
    print("=" * 70)
    print("Running Filesystem benchmark with vLLM functiongemma-270m-it model")
    print("=" * 70)

    # Set up trace collector to log all interactions
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"log/mcpmark/filesystem_vllm_trace_{timestamp}.log"
    trace_collector = FileCollector(log_file=log_file)

    print(f"\nTrace log will be saved to: {log_file}")

    # Load benchmark config
    benchmark = BenchmarkRunner("mcpmark/configs/mcpmark_filesystem_vllm.yaml")

    print("\nStarting benchmark run...")
    print("-" * 70)

    # Run with verbose callbacks to see real-time output
    benchmark_results = await benchmark.run(
        trace_collector=trace_collector,
        callbacks=get_vprint_callbacks()
    )

    print("\n" + "=" * 70)
    print("Benchmark Complete!")
    print("=" * 70)

    # Generate and save report
    report = BenchmarkReport(benchmark, trace_collector=trace_collector)
    report.dump()

    # Print evaluation results
    print("\n" + "=" * 70)
    print("Evaluation Results")
    print("-" * 70)

    total_passed = 0
    total_failed = 0

    for task_name in benchmark_results[0].task_results.keys():
        print(f"\nTask: {task_name}")
        print("-" * 50)
        eval_results = benchmark_results[0].task_results[task_name]['evaluation_results']
        for eval_result in eval_results:
            print(f"  func: {eval_result.config.func}")
            print(f"  op: {eval_result.config.op}")
            print(f"  value: {eval_result.config.value}")
            passed = eval_result.passed
            if passed:
                print(f"  Passed: \033[32mTrue\033[0m")
                total_passed += 1
            else:
                print(f"  Passed: \033[31mFalse\033[0m")
                total_failed += 1
        print("-" * 50)

    print(f"\n" + "=" * 70)
    print(f"Summary: {total_passed} passed, {total_failed} failed")
    print(f"Trace log saved to: {log_file}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
