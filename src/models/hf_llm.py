from __future__ import annotations

from langchain_huggingface import HuggingFacePipeline
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline

from config.pipeline_context import PipelineContext
from config.settings import Params


class LLMGenerator:
    """
    Summary: Handles HuggingFace LLM model and tokeniser initialisation
    """

    def __init__(
        self,
        ctx: PipelineContext,
    ):
        self.ctx = ctx
        self.params: Params = ctx.settings.params

    def hf_gen_pipeline(self):
        model_name = self.params.language_model_name
        tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            truncation=self.params.truncation,
            model_max_length=self.params.max_input_seq_length,
        )
        model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        hf_pipeline = pipeline(
            "text2text-generation",
            model=model,
            tokenizer=tokenizer,
            max_length=self.params.max_output_seq_length,
        )
        return HuggingFacePipeline(pipeline=hf_pipeline)
