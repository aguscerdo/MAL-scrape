import MAL_api as mal

if __name__ == "__main__":
    ex = mal.Extracter(file=True)
    ex.retrieve(start_i=-1, fail_limit=4000, sleep=True, verbose=True)

    # ex.update_producers(verbose=True)
