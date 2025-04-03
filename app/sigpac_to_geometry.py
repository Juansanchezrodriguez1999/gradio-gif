from sigpac_tools.find import find_from_cadastral_registry


def _convert_sigpac_to_cadastral_reference(
    province: int,
    municipality: int,
    polygon: int,
    parcel: int,
    precinct: int,
    section: str = "A",
) -> str:
    """
    Converts SIGPAC codes to a cadastral reference.

    The expected format for the cadastral reference is as follows:
        - 2 digits for the province code (padded with leading zeros if necessary)
        - 3 digits for the municipality code (padded with leading zeros if necessary)
        - 1 character for the cadastral section (default is "A")
        - 3 digits for the polygon code (padded with leading zeros if necessary)
        - 5 digits for the parcel code (padded with leading zeros if necessary)
        - 4 digits for the SIGPAC precinct code
    
    Args:
        province (int): Province code (2 digits).
        municipality (int): Municipality code (3 digits).
        polygon (int): Polygon code (3 digits).
        parcel (int): Parcel code (5 digits).
        precinct (int): Precinct code (4 digits).
        section (str): Cadastral section (1 character, default is "A").

    Returns:
        str: Cadastral reference in the specified format.
    """
    province_str = str(province).zfill(2)
    municipality_str = str(municipality).zfill(3)
    polygon_str = str(polygon).zfill(3)
    parcel_str = str(parcel).zfill(5)
    precinct_str = str(precinct).zfill(4)

    cadastral_reference = f"{province_str}{municipality_str}{section}{polygon_str}{parcel_str}{precinct_str}"
    return cadastral_reference


def _generate_cadastral_reference(reference: str) -> str:
    """
    Generates a complete cadastral reference with control characters.

    Args:
        reference (str): Base cadastral reference (without control characters).

    Returns:
        str: Complete cadastral reference with control characters.

    Raises:
        ValueError: If the base cadastral reference does not have a length of 18 characters.
        NotImplementedError: If the cadastral reference is for an urban property.
    """
    if len(reference) != 18:
        raise ValueError("The cadastral reference must have a length of 18 characters")

    position_weights = [13, 15, 12, 5, 4, 17, 9, 21, 3, 7, 1]
    control_characters = "MQWERTYUIOPASDFGHJKLBZX"

    sum_primary_digits = 0
    sum_secondary_digits = 0
    mixed_sum = 0

    reference = reference.upper().replace(" ", "")
    separated_ref = list(reference)

    for i in range(7):
        sum_primary_digits += _calculate_character_value(
            separated_ref[i], position_weights[i]
        )
        sum_secondary_digits += _calculate_character_value(
            separated_ref[i + 7], position_weights[i]
        )

    for i in range(4):
        mixed_sum += position_weights[i + 7] * (ord(separated_ref[i + 14]) - 48)

    code_pos1 = (sum_primary_digits + mixed_sum) % 23
    code_pos2 = (sum_secondary_digits + mixed_sum) % 23

    control_char1 = control_characters[code_pos1]
    control_char2 = control_characters[code_pos2]

    complete_cadastral_reference = f"{reference}{control_char1}{control_char2}"

    property_type = "URBAN" if separated_ref[5].isdigit() else "RURAL"

    if property_type == "URBAN":
        raise NotImplementedError(
            "Urban cadastral references are not supported yet. Please check the reference and try again."
        )

    return complete_cadastral_reference


def _calculate_character_value(char: str, position_weight: int) -> int:
    """
    Calculates the numerical value of a character based on its ASCII value and position weight.

    Args:
        char (str): Character to be evaluated.
        position_weight (int): Weight corresponding to the character's position.

    Returns:
        int: Calculated value of the character based on its position weight.
    """
    if char.isdigit():
        return position_weight * (ord(char) - 48)
    else:
        if ord(char) > 78:
            return position_weight * (ord(char) - 63)
        else:
            return position_weight * (ord(char) - 64)


def sigpac_to_geometry(
    province: int, municipality: int, polygon: int, parcel: int, precinct: int
):
    """
    Converts SIGPAC codes to geometrical data by mapping them to a cadastral reference.

    Args:
        province (int): Province code.
        municipality (int): Municipality code.
        polygon (int): Polygon code.
        parcel (int): Parcel code.
        precinct (int): Precinct code.

    Returns:
        tuple: A tuple containing the geometry and metadata related to the cadastral reference.
    """
    cadastral_reference = _convert_sigpac_to_cadastral_reference(
        province=province,
        municipality=municipality,
        polygon=polygon,
        parcel=parcel,
        precinct=precinct,
    )
    complete_cadastral_reference = _generate_cadastral_reference(cadastral_reference)
    geometry, metadata = find_from_cadastral_registry(complete_cadastral_reference)
    return geometry, metadata
