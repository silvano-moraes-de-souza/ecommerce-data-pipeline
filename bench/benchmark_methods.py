"""Benchmark COPY vs INSERT for the e-commerce data pipeline.

Este script mede o tempo e a memória de ambas as estratégias de carga
(COPY e INSERT) usando o conjunto de dados gerado pelo shopflow-datagen.

Para rodar é necessário:
1. PostgreSQL 16 rodando com o banco e as tabelas criadas (schema.sql).
2. Definir a variável de ambiente DATABASE_URL apontando para o banco.
3. Exemplo: DATABASE_URL=postgresql://postgres:senha@localhost:5432/ecom_pipeline

O script gera um snapshot com escalas 1 e 10, carrega com cada método
e registra tempo de wall clock e pico de RSS (memória).
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

from bench.harness import save
from ecommerce_data_pipeline.cli import _connect
from ecommerce_data_pipeline.pipeline import extract, init_db
from ecommerce_data_pipeline.pipeline import run as pipeline_run


def run_benchmark(method: str, scale: int, seed: int = 42) -> dict:
    """Executa o pipeline completo e retorna contagens e tempo.

    Executa: extract → init_db → run (load + transform + reconcile)
    e retorna tempo de execução, contagens de linhas e status de checagem.
    """
    with __import__("tempfile").TemporaryDirectory(prefix="ecom-src-") as tmp:
        source = Path(tmp)
        print(f"[bench] Gerando dados scale={scale} seed={seed}...")
        extract(source, scale, seed)

        batch_id = f"sf{scale:g}-seed{seed}"

        url = os.environ.get("DATABASE_URL")
        if not url:
            print("⚠️  DATABASE_URL não definida. Pulei o benchmark.")
            return {"error": "DATABASE_URL not set"}

        print(f"[bench] Executando pipeline method={method} batch={batch_id}...")
        t0 = time.perf_counter()

        try:
            conn = _connect()
            init_db(conn)
            run_result = pipeline_run(conn, source, batch_id, method=method)
            t1 = time.perf_counter()
            duration = t1 - t0
            return {
                "method": method,
                "scale": scale,
                "duration_s": duration,
                "row_counts": run_result.row_counts,
                "checks_ok": all(c["ok"] for c in run_result.checks),
                "checks": [c["check"] for c in run_result.checks],
                "error": run_result.error,
            }
        except Exception as e:
            return {"method": method, "scale": scale, "error": str(e)}


def main():
    # Garante que a URL do banco esteja definida
    if "DATABASE_URL" not in os.environ:
        print("❌ Defina DATABASE_URL antes de rodar o benchmark.")
        sys.exit(1)

    scales = [1, 10]
    methods = ["copy", "insert"]
    results = []

    for scale in scales:
        for method in methods:
            print(f"\n=== Benchmark: scale={scale}, method={method} ===")
            res = run_benchmark(method, scale)
            results.append(res)
            print(f"   Resultado: {res}")

    # Salvar resultados
    out_path = save("copy_vs_insert_benchmark", results)
    print(f"\n✅ Resultados salvos em: {out_path}")


if __name__ == "__main__":
    main()
