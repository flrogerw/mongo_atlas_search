"use client";

import React, { useCallback, ChangeEvent, useState, FormEvent } from "react";
import { useDebouncedCallback } from "use-debounce";
import LoadingSpinner from "@/components/LoadingSpinner";

enum Form {
  Initial,
  Loading,
  Success,
  Error,
}

type FormState = {
  state: Form;
  message?: string;
};

export type Match = {
  id: string;
  title: string;
  description: string;
  audioUrl: string;
};

async function wait(s: number) {
  return new Promise((resolve) => setTimeout(resolve, s * 1000));
}

export default function Home() {
  const [form, setForm] = useState<FormState>({ state: Form.Initial });
  const [matches, setMatches] = useState<Match[]>([]);
  const search = useDebouncedCallback(
    async (e: ChangeEvent<HTMLInputElement>) => {
      setForm({ state: Form.Loading });
      console.log("searching: ", e.target.value);
      // Do stuff
      await fetch("https://google.com", {
        mode: "no-cors",
      });
      setMatches([]);
      setForm({ state: Form.Success });
    },
    250
  );
  return (
    <main className="flex min-h-screen flex-col items-center justify-between p-24">
      <div className="flex flex-col items-center justify-center w-full max-w-4xl mx-auto mb-16">
        <h1 className="text-4xl sm:text-6xl font-bold text-gray-800 dark:text-gray-200 mb-8">
          Audacy Search
        </h1>
        <div className="relative w-full mb-4">
          <input
            aria-label="Search articles"
            type="text"
            onChange={search}
            placeholder="Search for something..."
            className="block w-full px-4 py-2 text-gray-900 bg-white border border-gray-200 rounded-md dark:border-gray-900 focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-800 dark:text-gray-100"
          />
          <button type="submit">
            {form.state === Form.Loading ? (
              <LoadingSpinner className="absolute right-3 top-3" />
            ) : (
              <svg
                className="absolute w-5 h-5 text-gray-400 right-3 top-3 dark:text-gray-300"
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                />
              </svg>
            )}
          </button>
        </div>
        {matches.length ? (
          <div>
            <h2 className="mb-6 text-3xl font-bold tracking-tight text-black md:text-5xl dark:text-white">
              Matches
            </h2>
            {matches.map((m) => {
              return (
                <div
                  key={m.id}
                  className="relative border border-grey-200 dark:border-gray-800 rounded p-4 mb-6"
                >
                  <h4 className="mb-4 text-lg text-gray-900 dark:text-gray-100">
                    {m.title}
                  </h4>
                  <p className="text-gray-600 dark:text-gray-400 mb-4">
                    &quot;{m.description}&quot;
                  </p>
                  <div className="max-sm:mb-4">
                    <audio className="w-full" controls src={m.audioUrl}>
                      Your browser does not support the
                      <code>audio</code> element.
                    </audio>
                  </div>
                </div>
              );
            })}
          </div>
        ) : null}
      </div>
    </main>
  );
}
