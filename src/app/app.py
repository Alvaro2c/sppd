import streamlit as st
import src.dl_parser.mappings as mappings


def main():
    st.title("Simple Streamlit App")
    st.write("Welcome to your first Streamlit app!")

    name = st.text_input("Enter your name:")
    if name:
        st.write(f"Hello, {name}!")
        st.write("Here are some mappings:")
        st.write(mappings)


if __name__ == "__main__":
    main()
